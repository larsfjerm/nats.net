﻿// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Diagnostics;

namespace NATS.Client.JetStream
{
    public abstract class JetStreamAbstractSyncSubscription : SyncSubscription
    {
        internal IAutoStatusManager _asm;
        public JetStream Context { get; }
        public string Stream { get; }
        public string Consumer { get; }
        public string DeliverSubject { get; }

        internal JetStreamAbstractSyncSubscription(ISubscription sub,
            IAutoStatusManager asm, JetStream js, string stream, string consumer, string deliver)
            : base(sub.Connection, sub.Sid, sub.Subject, sub.Queue)
        {
            _asm = asm;
            Context = js;
            Stream = stream;
            Consumer = consumer;
            DeliverSubject = deliver;
        }

        public ConsumerInfo GetConsumerInformation() => Context.LookupConsumerInfo(Stream, Consumer);
                
        public override void Unsubscribe()
        {
            _asm.Shutdown();
            base.Unsubscribe();
        }

        public override void Close()
        {
            _asm.Shutdown();
            base.Close();
        }

        public new Msg NextMessage()
        {
            // this calls is intended to block indefinitely 
            Msg msg = base.NextMessage(-1);
            while (msg != null && _asm.Manage(msg)) {
                msg = base.NextMessage(-1);
            }
            return msg;
        }

        public new Msg NextMessage(int timeout)
        {
            if (timeout < 0)
            {
                return NextMessage();
            }

            Stopwatch sw = Stopwatch.StartNew();
            long leftover = timeout;
            while (leftover > 0) {
                Msg msg = base.NextMessage(timeout);
                if (!_asm.Manage(msg)) { // not managed means JS Message
                    return msg;
                }
                leftover = timeout - (int)sw.ElapsedMilliseconds;
            }
            throw new NATSTimeoutException();
        }
    }
}
