// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using NATS.Client;
using System.Threading;
using System.IO;
using Xunit;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;
using System.Text;

namespace IntegrationTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestEncoding : TestSuite<EncodingSuiteContext>
    {
        public TestEncoding(EncodingSuiteContext context) : base(context)
        {
        }

        public IEncodedConnection DefaultEncodedConnection => Context.OpenEncodedConnectionWithDefaultTimeout(Context.Server1.Port);

        [DataContract]
        public class JsonObject
        {
            [DataMember] public string Value = "";

            public JsonObject()
            {
            }

            public JsonObject(string val)
            {
                Value = val;
            }

            public override bool Equals(object obj)
            {
                return (((JsonObject) obj).Value == Value);
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }
        }

        [Fact]
        public void TestEncodedObjectSerization()
        {
            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IEncodedConnection c = DefaultEncodedConnection)
                {
                    c.OnDeserialize = jsonDeserializer;
                    c.OnSerialize = jsonSerializer;

                    AutoResetEvent ev = new AutoResetEvent(false);
                    JsonObject jo = new JsonObject("bar");

                    EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                    {
                        Assert.True(args.ReceivedObject.Equals(jo));
                        ev.Set();
                    };

                    using (c.SubscribeAsync("foo", eh))
                    {
                        for (int i = 0; i < 10; i++)
                            c.Publish("foo", jo);

                        c.Flush();

                        Assert.True(ev.WaitOne(1000));
                    }

                    ev.Reset();
                    using (c.SubscribeAsync("foo", eh))
                    {
                        c.Publish("foo", "bar", jo);
                        c.Flush();

                        Assert.True(ev.WaitOne(1000));
                    }
                }
            }
        }

        [Fact]
        public void TestEncodedInvalidObjectSerialization()
        {
            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IEncodedConnection c = DefaultEncodedConnection)
                {
                    AutoResetEvent ev = new AutoResetEvent(false);

                    c.OnSerialize = jsonSerializer;
                    c.OnDeserialize = jsonSerializer;

                    bool hitException = false;

                    EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                    {
                        // Ensure we blow up in the cast or not implemented in .NET core
                        try
                        {
                            Exception invalid = (Exception) args.ReceivedObject;
                        }
                        catch (Exception)
                        {
                            hitException = true;
                        }

                        ev.Set();
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        c.Publish("foo", new JsonObject("data"));
                        c.Flush();

                        ev.WaitOne(1000);

                        Assert.True(hitException);
                    }
                }
            }
        }

        internal object jsonDeserializer(byte[] buffer)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                var serializer = new DataContractJsonSerializer(typeof(JsonObject));
                stream.Write(buffer, 0, buffer.Length);
                stream.Position = 0;
                return serializer.ReadObject(stream);
            }
        }

        internal byte[] jsonSerializer(object obj)
        {
            if (obj == null)
                return null;

            var serializer = new DataContractJsonSerializer(typeof(JsonObject));

            using (MemoryStream stream = new MemoryStream())
            {
                serializer.WriteObject(stream, obj);

                ArraySegment<byte> buffer;
                if (stream.TryGetBuffer(out buffer))
                {
                    long len = stream.Position;
                    var rv = new byte[len];
                    Array.Copy(buffer.Array, rv, (int)len);
                    return rv;
                }
                else
                {
                    throw new Exception("Unable to serialize - buffer error");
                }
            }
        }

        [Fact]
        public void TestEncodedSerizationOverrides()
        {
            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IEncodedConnection c = DefaultEncodedConnection)
                {
                    c.OnDeserialize = jsonDeserializer;
                    c.OnSerialize = jsonSerializer;

                    AutoResetEvent ev = new AutoResetEvent(false);

                    JsonObject origObj = new JsonObject("bar");

                    EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                    {
                        JsonObject so = (JsonObject) args.ReceivedObject;
                        Assert.True(so.Equals(origObj));

                        ev.Set();
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        c.Publish("foo", origObj);
                        c.Flush();

                        ev.WaitOne(1000);
                    }
                }
            }
        }

        [Fact]
        public void TestEncodedObjectRequestReply()
        {
            using (NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IEncodedConnection c = DefaultEncodedConnection)
                {
                    c.OnDeserialize = jsonDeserializer;
                    c.OnSerialize = jsonSerializer;

                    JsonObject origObj = new JsonObject("foo");

                    EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                    {
                        JsonObject so = (JsonObject) args.ReceivedObject;
                        Assert.True(so.Equals(origObj));

                        c.Publish(args.Reply, new JsonObject("Received"));
                        c.Flush();
                    };

                    using (c.SubscribeAsync("foo", eh))
                    {
                        var jo = (JsonObject) c.Request("foo", origObj, 1000);
                        Assert.Equal("Received", jo.Value);

                        jo = (JsonObject) c.Request("foo", origObj, 1000);
                        Assert.Equal("Received", jo.Value);
                    }
                }
            }
        }
    } // class
} // namespace