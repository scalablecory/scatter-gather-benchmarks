using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace ScatterGatherTests
{
    /// <summary>
    /// Run server process with:
    /// dotnet run -c Release -- r
    /// 
    /// Run client process with:
    /// dotnet run -c Release -- b --host [::1]:55698
    /// 
    /// Every loop of the benchmark is given either a "." or "+".
    /// "." Indicates it is finding a better time.
    /// "+" Indicates it found a better time.
    /// 
    /// A benchmark loop ends when it can't find a better time after 5 tries.
    /// 
    /// Each benchmark simulates a HTTP/2 request:
    /// - Write headers
    /// - Write content bytes A (vector of buffers A1, A2)
    /// - Write content bytes B (single buffer B1)
    /// 
    /// Note: lengths returned from Write calls are assumed to always be the entire buffer,
    /// to mimic Stream.Write. Some of the implementation returns 0 instead of an actual value.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            System.Diagnostics.Process.GetCurrentProcess().PriorityClass = System.Diagnostics.ProcessPriorityClass.High;

            var rootCommand = new RootCommand("Benchmarks scatter/gather")
            {
                TreatUnmatchedTokensAsErrors = true,
            };

            var remoteCommand = new Command("r", "Runs the remote host.");
            remoteCommand.AddOption(new Option<string>("--ip", "The IP to bind to."));
            remoteCommand.AddOption(new Option<int>("--port", "The port to bind to."));
            remoteCommand.Handler = CommandHandler.Create<string, int>((ip, port) =>
            {
                if (ip == null) ip = "::1";
                RunRemoteHost(ip, port);
            });

            var benchmarkCommand = new Command("b", "Runs the benchmark.");
            benchmarkCommand.AddOption(new Option<string>("--host", "The host to connect to.")
            {
                IsRequired = true
            });
            benchmarkCommand.Handler = CommandHandler.Create<string>(host =>
            {
                new Program().RunBenchmarkAsync(host).AsTask().GetAwaiter().GetResult();
            });

            rootCommand.AddCommand(remoteCommand);
            rootCommand.AddCommand(benchmarkCommand);
            rootCommand.Invoke(args);
        }

        // These buffers are the sequence of bytes to write.
        readonly byte[] _headers = new byte[64];
        readonly byte[] _chunkPrefixA = new byte[16];
        readonly byte[] _a1 = new byte[8];
        readonly byte[] _a2 = new byte[1024];
        readonly byte[] _chunkSuffixA = new byte[2];
        readonly byte[] _chunkPrefixB = new byte[16];
        byte[] _b1 = new byte[4096];
        readonly byte[] _chunkSuffixB = new byte[2];
        readonly byte[] _finalChunk = new byte[5];

        // These buffers are combinations of the above, that would be reasonably buffered together.
        readonly byte[] _headersAndChunkPrefixA;
        readonly byte[] _chunkSuffixBAndFinalChunk;

        // Mutable things used in benchmarks.
        readonly List<ArraySegment<byte>> _gatherBuffers = new List<ArraySegment<byte>>(16);
        readonly List<ArraySegment<byte>> _innerGatherBuffers = new List<ArraySegment<byte>>(16);

        readonly ValueTaskSocketAsyncEventArgs _gatherEventArgs = new ValueTaskSocketAsyncEventArgs();
        readonly byte[] _sendBuffer = new byte[4096];
        int _sendBufferFillLength = 0;

        Socket _socket;
        int _ioCount = 0;

        Program()
        {
            _headersAndChunkPrefixA = new byte[_headers.Length + _chunkPrefixA.Length];
            _chunkSuffixBAndFinalChunk = new byte[_chunkSuffixB.Length + _finalChunk.Length];
        }

        private static void RunRemoteHost(string ip, int port)
        {
            byte[] buffer = GC.AllocateUninitializedArray<byte>(64 * 1024, pinned: true);

            using var listenSocket = new Socket(SocketType.Stream, ProtocolType.Tcp);

            listenSocket.Bind(new IPEndPoint(IPAddress.Parse(ip), port));
            Console.WriteLine($"Remote host bound on {listenSocket.LocalEndPoint}");

            listenSocket.Listen();

            while (true)
            {
                try
                {
                    while (true)
                    {
                        using Socket socket = listenSocket.Accept();
                        while (socket.Receive(buffer, SocketFlags.None) != 0) ;
                    }
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
                {
                    // do nothing.
                }
            }
        }

        private async ValueTask RunBenchmarkAsync(string remoteHost)
        {
            Console.WriteLine("===== With B1 =====");
            await RunBenchmarkAsync(remoteHost, nameof(WriteUnbufferedAsync), WriteUnbufferedAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteUnbufferedGatheredAsync), WriteUnbufferedGatheredAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteBufferedAsync), WriteBufferedAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteBufferedGatheredAsync), WriteBufferedGatheredAsync).ConfigureAwait(false);

            Console.WriteLine("===== Without B1 =====");
            _b1 = Array.Empty<byte>();
            await RunBenchmarkAsync(remoteHost, nameof(WriteUnbufferedAsync), WriteUnbufferedAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteUnbufferedGatheredAsync), WriteUnbufferedGatheredAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteBufferedAsync), WriteBufferedAsync).ConfigureAwait(false);
            await RunBenchmarkAsync(remoteHost, nameof(WriteBufferedGatheredAsync), WriteBufferedGatheredAsync).ConfigureAwait(false);
        }

        private async ValueTask RunBenchmarkAsync(string remoteHost, string name, Func<ValueTask> asyncFunc)
        {
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);

            await socket.ConnectAsync(IPEndPoint.Parse(remoteHost)).ConfigureAwait(false);

            _socket = socket;

            Console.WriteLine($"[{name}] Warming up...");

            int startTicks = Environment.TickCount, totalTicks, runsPerSecond = 0, ioCount;

            do
            {
                _ioCount = 0;
                await asyncFunc().ConfigureAwait(false);
                ioCount = _ioCount;
                ++runsPerSecond;
                totalTicks = Environment.TickCount - startTicks;
            }
            while (totalTicks < 5000);

            Console.WriteLine($"[{name}] Warmup complete; executing {runsPerSecond} runs per loop.");

            Console.Write($"[{name}] Benchmarking...");

            int loopsWithNoChange = 0,
                bestTotalTicks = int.MaxValue;

            while (loopsWithNoChange != 5)
            {
                startTicks = Environment.TickCount;

                for (int i = 0; i < runsPerSecond; ++i)
                {
                    await asyncFunc().ConfigureAwait(false);
                }

                totalTicks = Environment.TickCount - startTicks;

                if (totalTicks < bestTotalTicks)
                {
                    bestTotalTicks = totalTicks;
                    loopsWithNoChange = 0;
                    Console.Write("+");
                }
                else
                {
                    ++loopsWithNoChange;
                    Console.Write(".");
                }
            }

            Console.WriteLine();
            Console.WriteLine($"[{name}] Benchmarking complete; best result: {runsPerSecond * 1000 / bestTotalTicks} runs per second, with {ioCount} I/Os per run. ");
        }

        /// <summary>
        /// Enveloping is buffered, but caller's writes are not.
        /// 
        /// - Write headers, chunk A prefix.
        /// - Write A1
        /// - Write A2
        /// - Write chunk A suffix, chunk B prefix
        /// - Write B1
        /// - Write chunk B suffix, final chunk
        /// </summary>
        public async ValueTask WriteUnbufferedAsync()
        {
            await WriteAsync(_headersAndChunkPrefixA).ConfigureAwait(false);
            await WriteAsync(_a1).ConfigureAwait(false);
            await WriteAsync(_a2).ConfigureAwait(false);
            await WriteAsync(_chunkSuffixA).ConfigureAwait(false);
            if (_b1.Length != 0)
            {
                await WriteAsync(_chunkPrefixB).ConfigureAwait(false);
                await WriteAsync(_b1).ConfigureAwait(false);
                await WriteAsync(_chunkSuffixBAndFinalChunk).ConfigureAwait(false);
            }
            else
            {
                await WriteAsync(_finalChunk).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Enveloping is buffered, but caller's writes are not.
        /// Gathered writes are used.
        /// 
        /// - Write headers, chunk A prefix, A1, A2, chunk A suffix
        /// - Write chunk B prefix, B1, chunk B suffix, final chunk
        /// </summary>
        public async ValueTask WriteUnbufferedGatheredAsync()
        {
            _gatherBuffers.Clear();
            _gatherBuffers.Add(_headersAndChunkPrefixA);
            _gatherBuffers.Add(_a1);
            _gatherBuffers.Add(_a2);
            _gatherBuffers.Add(_chunkSuffixA);

            if (_b1.Length != 0)
            {
                await WriteAsync(_gatherBuffers).ConfigureAwait(false);

                _gatherBuffers.Clear();
                _gatherBuffers.Add(_chunkPrefixB);
                _gatherBuffers.Add(_b1);
                _gatherBuffers.Add(_chunkSuffixBAndFinalChunk);
            }
            else
            {
                _gatherBuffers.Add(_finalChunk);
            }

            await WriteAsync(_gatherBuffers).ConfigureAwait(false);
        }

        /// <summary>
        /// Enveloping and caller's writes are buffered together and flushed in chunks.
        /// </summary>
        public async ValueTask WriteBufferedAsync()
        {
            await WriteBufferedAsync(_headersAndChunkPrefixA).ConfigureAwait(false);
            await WriteBufferedAsync(_a1).ConfigureAwait(false);
            await WriteBufferedAsync(_a2).ConfigureAwait(false);
            await WriteBufferedAsync(_chunkSuffixA).ConfigureAwait(false);
            if (_b1.Length != 0)
            {
                await WriteBufferedAsync(_chunkPrefixB).ConfigureAwait(false);
                await WriteBufferedAsync(_b1).ConfigureAwait(false);
                await WriteBufferedAsync(_chunkSuffixBAndFinalChunk, flush: true).ConfigureAwait(false);
            }
            else
            {
                await WriteBufferedAsync(_finalChunk, flush: true).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Enveloping and caller's writes are buffered together and flushed in chunks.
        /// Gathered writes are used.
        /// </summary>
        public async ValueTask WriteBufferedGatheredAsync()
        {
            _gatherBuffers.Clear();
            _gatherBuffers.Add(_headersAndChunkPrefixA);
            _gatherBuffers.Add(_a1);
            _gatherBuffers.Add(_a2);
            _gatherBuffers.Add(_chunkSuffixA);

            if (_b1.Length != 0)
            {
                await WriteBufferedGatheredAsync(_gatherBuffers).ConfigureAwait(false);

                _gatherBuffers.Clear();
                _gatherBuffers.Add(_chunkPrefixB);
                _gatherBuffers.Add(_b1);
                _gatherBuffers.Add(_chunkSuffixBAndFinalChunk);
            }
            else
            {
                _gatherBuffers.Add(_finalChunk);
            }

            await WriteBufferedGatheredAsync(_gatherBuffers, flush: true).ConfigureAwait(false);
        }

        /// <summary>
        /// Simulates copying to the send buffer; what you would do for user's content buffers.
        /// </summary>
        ValueTask<int> WriteBufferedAsync(ArraySegment<byte> buffer, bool flush = false)
        {
            int available = _sendBuffer.Length - _sendBufferFillLength;

            if (available > buffer.Count && !flush)
            {
                buffer.CopyTo(_sendBuffer, _sendBufferFillLength);
                _sendBufferFillLength += buffer.Count;
                return new ValueTask<int>(0);
            }

            if (_sendBufferFillLength == 0)
            {
                return WriteAsync(buffer);
            }

            if (available > buffer.Count)
            {
                buffer.CopyTo(_sendBuffer, _sendBufferFillLength);

                int sendLen = _sendBufferFillLength + buffer.Count;
                _sendBufferFillLength = 0;

                return WriteAsync(_sendBuffer.AsMemory(0, sendLen));
            }

            if (!flush)
            {
                int remainingBufferLength = buffer.Count - available;

                if (remainingBufferLength < _sendBuffer.Length)
                {
                    buffer.AsSpan(0, available).CopyTo(_sendBuffer.AsSpan(_sendBufferFillLength));
                    return SendThenBufferAsync(buffer.Slice(available));
                }
            }

            return SendThenSendAsync(buffer);
        }

        async ValueTask<int> SendThenBufferAsync(ArraySegment<byte> buffer)
        {
            await WriteAsync(_sendBuffer).ConfigureAwait(false);

            buffer.CopyTo(_sendBuffer);
            _sendBufferFillLength = buffer.Count;

            return 0;
        }

        async ValueTask<int> SendThenSendAsync(ArraySegment<byte> buffer)
        {
            await WriteAsync(_sendBuffer.AsMemory(0, _sendBufferFillLength)).ConfigureAwait(false);
            await WriteAsync(buffer).ConfigureAwait(false);

            _sendBufferFillLength = 0;
            return 0;
        }

        /// <summary>
        /// Simulates copying to the send buffer; what you would do for user's content buffers.
        /// </summary>
        ValueTask<int> WriteBufferedGatheredAsync(IList<ArraySegment<byte>> buffers, bool flush = false)
        {
            int bufferCount = buffers.Count;
            int totalSendLen = 0;

            for (int i = 0; i != bufferCount; ++i)
            {
                totalSendLen += buffers[i].Count;
            }

            if (_sendBuffer.Length - _sendBufferFillLength >= totalSendLen && !flush)
            {
                for (int i = 0; i != bufferCount; ++i)
                {
                    ArraySegment<byte> buffer = buffers[i];
                    buffer.CopyTo(_sendBuffer, _sendBufferFillLength);
                    _sendBufferFillLength += buffer.Count;
                }

                return ValueTask.FromResult(totalSendLen);
            }

            if (_sendBufferFillLength == 0)
            {
                return WriteAsync(buffers);
            }

            _innerGatherBuffers.Clear();
            _innerGatherBuffers.Add(new ArraySegment<byte>(_sendBuffer, 0, _sendBufferFillLength));
            _innerGatherBuffers.AddRange(buffers);
            _sendBufferFillLength = 0;
            return WriteAsync(_innerGatherBuffers);
        }

        ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer)
        {
            ++_ioCount;
            return _socket.SendAsync(buffer, SocketFlags.None);
        }

        ValueTask<int> WriteAsync(IList<ArraySegment<byte>> buffers)
        {
            ++_ioCount;

            _gatherEventArgs.BufferList = buffers;
            _gatherEventArgs.PrepareForOperation();

            if (_socket.SendAsync(_gatherEventArgs))
            {
                return _gatherEventArgs.Task;
            }

            if (_gatherEventArgs.SocketError == SocketError.Success)
            {
                return new ValueTask<int>(_gatherEventArgs.BytesTransferred);
            }
            
            return ValueTask.FromException<int>(new SocketException((int)_gatherEventArgs.SocketError));
        }
    }

    sealed class ValueTaskSocketAsyncEventArgs : SocketAsyncEventArgs, IValueTaskSource<int>
    {
        ManualResetValueTaskSourceCore<int> _taskSource;

        public ValueTask<int> Task =>
            new ValueTask<int>(this, _taskSource.Version);

        public void PrepareForOperation() =>
            _taskSource.Reset();

        protected override void OnCompleted(SocketAsyncEventArgs e)
        {
            if (SocketError == SocketError.Success)
            {
                _taskSource.SetResult(e.BytesTransferred);
            }
            else
            {
                _taskSource.SetException(new SocketException((int)SocketError));
            }
        }

        int IValueTaskSource<int>.GetResult(short token) =>
            _taskSource.GetResult(token);

        ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) =>
            _taskSource.GetStatus(token);

        void IValueTaskSource<int>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _taskSource.OnCompleted(continuation, state, token, flags);
    }
}
