/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.record.send.Send;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbPhysicalTablePath;
import org.apache.fluss.rpc.netty.client.ClientHandlerCallback;
import org.apache.fluss.rpc.netty.client.NettyClientHandler;
import org.apache.fluss.rpc.netty.server.FlussRequest;
import org.apache.fluss.rpc.netty.server.NettyServerHandler;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.security.auth.PlainTextAuthenticationPlugin;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelId;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.EventExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.ByteBufChannel.toByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link MessageCodec}. */
class MessageCodecTest {

    private NettyClientHandler clientHandler;
    private RequestChannel requestChannel;
    private NettyServerHandler serverHandler;
    private ResponseReceiver responseReceiver;
    private ChannelHandlerContext ctx;

    @BeforeEach
    void beforeEach() {
        this.responseReceiver = new ResponseReceiver();
        this.clientHandler = new NettyClientHandler(responseReceiver);
        this.requestChannel = new RequestChannel(100);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        this.serverHandler =
                new NettyServerHandler(
                        requestChannel,
                        new ApiManager(ServerType.TABLET_SERVER),
                        "FLUSS",
                        true,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup),
                        new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
        this.ctx = mockChannelHandlerContext();
    }

    @Test
    void testEncodeRequest() throws Exception {
        ApiVersionsRequest request = new ApiVersionsRequest();
        request.setClientSoftwareName("test").setClientSoftwareVersion("1.0.0");
        ByteBuf byteBuf =
                MessageCodec.encodeRequest(
                        ByteBufAllocator.DEFAULT,
                        ApiKeys.API_VERSIONS.id,
                        ApiKeys.API_VERSIONS.highestSupportedVersion,
                        1001,
                        request);
        serverHandler.channelRead(ctx, byteBuf);

        FlussRequest rpcRequest = (FlussRequest) requestChannel.pollRequest(1000);
        assertThat(rpcRequest).isNotNull();
        assertThat(rpcRequest.getApiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);
        assertThat(rpcRequest.getApiVersion())
                .isEqualTo(ApiKeys.API_VERSIONS.highestSupportedVersion);
        assertThat(rpcRequest.getRequestId()).isEqualTo(1001);
        assertThat(rpcRequest.getMessage().totalSize()).isEqualTo(request.totalSize());
        assertThat(rpcRequest.getMessage()).isInstanceOf(ApiVersionsRequest.class);

        ApiVersionsRequest actual = (ApiVersionsRequest) rpcRequest.getMessage();
        assertThat(actual.getClientSoftwareName()).isEqualTo("test");
        assertThat(actual.getClientSoftwareVersion()).isEqualTo("1.0.0");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodeSuccessResponse() throws Exception {
        ApiVersionsResponse response = new ApiVersionsResponse();
        PbApiVersion apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response.addAllApiVersions(Collections.singletonList(apiVersion));
        Send send = MessageCodec.encodeSuccessResponse(ByteBufAllocator.DEFAULT, 1001, response);
        ByteBuf byteBuf = toByteBuf(send);
        clientHandler.channelRead(ctx, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(1001);
        assertThat(responseReceiver.response).isNotNull();
        assertThat(responseReceiver.response).isInstanceOf(ApiVersionsResponse.class);
        ApiVersionsResponse actualResp = (ApiVersionsResponse) responseReceiver.response;
        assertThat(actualResp.getApiVersionsCount()).isEqualTo(1);
        PbApiVersion actualApiVersion = actualResp.getApiVersionAt(0);
        assertThat(actualApiVersion.getApiKey()).isEqualTo(apiVersion.getApiKey());
        assertThat(actualApiVersion.getMinVersion()).isEqualTo(apiVersion.getMinVersion());
        assertThat(actualApiVersion.getMaxVersion()).isEqualTo(apiVersion.getMaxVersion());
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodeErrorResponse() throws Exception {
        ApiError error = new ApiError(Errors.NETWORK_EXCEPTION, "response error");
        ByteBuf byteBuf = MessageCodec.encodeErrorResponse(ByteBufAllocator.DEFAULT, 1001, error);
        clientHandler.channelRead(null, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(1001);
        assertThat(responseReceiver.response).isNull();
        assertThat(responseReceiver.responseError)
                .isNotNull()
                .isInstanceOf(Errors.NETWORK_EXCEPTION.exception().getClass())
                .hasMessage("response error");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testEncodingServerFailure() throws Exception {
        ApiError error = new ApiError(Errors.CORRUPT_MESSAGE, "server error");
        ByteBuf byteBuf = MessageCodec.encodeServerFailure(ByteBufAllocator.DEFAULT, error);
        clientHandler.channelRead(ctx, byteBuf);

        assertThat(responseReceiver.requestId).isEqualTo(-1);
        assertThat(responseReceiver.response).isNull();
        assertThat(responseReceiver.serverError)
                .isNotNull()
                .isInstanceOf(Errors.CORRUPT_MESSAGE.exception().getClass())
                .hasMessage("server error");
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    @Test
    void testBulkLoadTopLevelMessagesRoundTrip() throws Exception {
        List<MessagePair> messages = bulkLoadMessages();
        assertThat(messages)
                .extracting(message -> message.api)
                .containsExactly(
                        ApiKeys.BEGIN_BULK_LOAD, ApiKeys.COMMIT_BULK_LOAD, ApiKeys.ABORT_BULK_LOAD);

        int requestId = 2000;
        for (MessagePair message : messages) {
            assertRequestRoundTrip(message.api, requestId, message.request);
            assertResponseRoundTrip(message.api, requestId, message.response);
            requestId++;
        }
    }

    private void assertRequestRoundTrip(ApiKeys api, int requestId, ApiMessage expected)
            throws Exception {
        NettyServerHandler handler = createServerHandler(ServerType.COORDINATOR);
        ByteBuf byteBuf =
                MessageCodec.encodeRequest(
                        ByteBufAllocator.DEFAULT,
                        api.id,
                        api.highestSupportedVersion,
                        requestId,
                        expected);
        handler.channelRead(ctx, byteBuf);

        FlussRequest actual = (FlussRequest) requestChannel.pollRequest(1000);
        assertThat(actual).isNotNull();
        assertThat(actual.getApiKey()).isEqualTo(api.id);
        assertThat(actual.getApiVersion()).isEqualTo(api.highestSupportedVersion);
        assertThat(actual.getRequestId()).isEqualTo(requestId);
        assertThat(actual.getMessage()).isExactlyInstanceOf(expected.getClass());
        assertThat(actual.getMessage().toByteArray()).containsExactly(expected.toByteArray());
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    private void assertResponseRoundTrip(ApiKeys api, int requestId, ApiMessage expected)
            throws Exception {
        ResponseReceiver receiver = new ResponseReceiver(ApiManager.forApiKey(api.id));
        NettyClientHandler handler = new NettyClientHandler(receiver);
        Send send =
                MessageCodec.encodeSuccessResponse(ByteBufAllocator.DEFAULT, requestId, expected);
        ByteBuf byteBuf = toByteBuf(send);
        handler.channelRead(ctx, byteBuf);

        assertThat(receiver.requestId).isEqualTo(requestId);
        assertThat(receiver.response).isExactlyInstanceOf(expected.getClass());
        assertThat(receiver.response.toByteArray()).containsExactly(expected.toByteArray());
        assertThat(byteBuf.readerIndex()).isEqualTo(byteBuf.writerIndex());
    }

    private NettyServerHandler createServerHandler(ServerType serverType) {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        return new NettyServerHandler(
                requestChannel,
                new ApiManager(serverType),
                "FLUSS",
                true,
                RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup),
                new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
    }

    private static List<MessagePair> bulkLoadMessages() {
        return Arrays.asList(
                new MessagePair(
                        ApiKeys.BEGIN_BULK_LOAD,
                        new BeginBulkLoadRequest()
                                .setTarget(physicalPath())
                                .setCallerToken("caller-token")
                                .setBuildTimeoutMs(30_000L),
                        new BeginBulkLoadResponse()
                                .setCreated(true)
                                .setStatus(status())
                                .setTargetInfo(targetInfo())),
                new MessagePair(
                        ApiKeys.COMMIT_BULK_LOAD,
                        new CommitBulkLoadRequest()
                                .setHandle(handle())
                                .setManifestLength(321L)
                                .setManifestSha256("manifest-sha")
                                .setManifestPath("oss://bucket/manifest"),
                        new CommitBulkLoadResponse().setStatus(status())),
                new MessagePair(
                        ApiKeys.ABORT_BULK_LOAD,
                        new AbortBulkLoadRequest().setHandle(handle()),
                        new AbortBulkLoadResponse().setStatus(status())));
    }

    private static PbPhysicalTablePath physicalPath() {
        return new PbPhysicalTablePath()
                .setDatabaseName("db")
                .setTableName("table")
                .setPartitionName("partition");
    }

    private static PbBulkLoadHandle handle() {
        return new PbBulkLoadHandle()
                .setTarget(physicalPath())
                .setTableId(11L)
                .setPartitionId(12L)
                .setBulkLoadId("bulk-load-id");
    }

    private static PbBulkLoadStatus status() {
        return new PbBulkLoadStatus()
                .setHandle(handle())
                .setState(3)
                .setAbortReason(0)
                .setAbortMessage("target is not empty");
    }

    private static PbBulkLoadTargetInfo targetInfo() {
        return new PbBulkLoadTargetInfo()
                .setHandle(handle())
                .setSchemaId(13)
                .setTableJson(new byte[] {9, 0, -9})
                .setCreatedTime(100L)
                .setModifiedTime(200L)
                .setRemoteDataDir("oss://bucket/data")
                .setSnapshotIds(new long[] {99L});
    }

    // ------------------------------------------------------------------------------------

    private static class ResponseReceiver implements ClientHandlerCallback {

        private final ApiMethod apiMethod;
        int requestId = -1;
        ApiMessage response;
        Throwable responseError;
        Throwable serverError;

        private ResponseReceiver() {
            this(ApiManager.forApiKey(ApiKeys.API_VERSIONS.id));
        }

        private ResponseReceiver(ApiMethod apiMethod) {
            this.apiMethod = apiMethod;
        }

        @Override
        public ApiMethod getRequestApiMethod(int requestId) {
            return apiMethod;
        }

        @Override
        public void onRequestResult(int requestId, ApiMessage response) {
            this.requestId = requestId;
            this.response = response;
        }

        @Override
        public void onRequestFailure(int requestId, Throwable cause) {
            this.requestId = requestId;
            this.responseError = cause;
        }

        @Override
        public void onFailure(Throwable cause) {
            this.serverError = cause;
        }
    }

    private static ChannelHandlerContext mockChannelHandlerContext() {
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("short_text");
        when(channelId.asLongText()).thenReturn("long_text");
        Channel channel = mock(Channel.class);
        when(channel.id()).thenReturn(channelId);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        EventExecutor eventExecutor = mock(EventExecutor.class);
        when(ctx.executor()).thenReturn(eventExecutor);
        return ctx;
    }

    private static final class MessagePair {
        private final ApiKeys api;
        private final ApiMessage request;
        private final ApiMessage response;

        private MessagePair(ApiKeys api, ApiMessage request, ApiMessage response) {
            this.api = api;
            this.request = request;
            this.response = response;
        }
    }
}
