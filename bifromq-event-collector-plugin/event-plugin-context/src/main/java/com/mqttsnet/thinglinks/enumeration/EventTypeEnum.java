package com.mqttsnet.thinglinks.enumeration;


import java.util.Optional;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @program: thinglinks
 * @description: MQTT事件枚举
 * @author: mqttsnet
 * @e-mainl: mqttsnet@163.com
 * @date: 2022-12-16 19:42
 **/
@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum EventTypeEnum {
    // Channel close related events
    AUTH_ERROR("AUTH_ERROR", "Authentication Error", "Client authentication failed", ""),
    ENHANCED_AUTH_ABORT_BY_CLIENT("ENHANCED_AUTH_ABORT_BY_CLIENT", "Enhanced Auth Aborted", "Enhanced authentication aborted by client", ""),
    UNAUTHENTICATED_CLIENT("UNAUTHENTICATED_CLIENT", "Unauthenticated Client", "Client failed authentication", ""),
    NOT_AUTHORIZED_CLIENT("NOT_AUTHORIZED_CLIENT", "Not Authorized Client", "Client not authorized to connect", ""),
    CHANNEL_ERROR("CHANNEL_ERROR", "Channel Error", "Channel-level error occurred", ""),
    CONNECT_TIMEOUT("CONNECT_TIMEOUT", "Connect Timeout", "Client connection timed out", ""),
    IDENTIFIER_REJECTED("IDENTIFIER_REJECTED", "Identifier Rejected", "Client identifier rejected (exceeds max length)", ""),
    MALFORMED_CLIENT_IDENTIFIER("MALFORMED_CLIENT_IDENTIFIER", "Malformed Client ID", "Client identifier has malformed UTF-8", ""),
    PROTOCOL_ERROR("PROTOCOL_ERROR", "Protocol Error", "MQTT protocol violation", ""),
    MALFORMED_USERNAME("MALFORMED_USERNAME", "Malformed Username", "Username has malformed UTF-8", ""),
    MALFORMED_WILL_TOPIC("MALFORMED_WILL_TOPIC", "Malformed Will Topic", "Will topic has malformed UTF-8", ""),
    UNACCEPTED_PROTOCOL_VER("UNACCEPTED_PROTOCOL_VER", "Unsupported Protocol", "Unsupported protocol version", ""),

    // Client connected
    CLIENT_CONNECTED("CLIENT_CONNECTED", "Client Connected", "Client successfully connected", "CONNECT"),

    // Client disconnect related
    BAD_PACKET("BAD_PACKET", "Bad Packet", "Received malformed MQTT packet", ""),
    BY_CLIENT("BY_CLIENT", "Client Disconnect", "Client initiated disconnect", "DISCONNECT"),
    BY_SERVER("BY_SERVER", "Server Disconnect", "Server initiated disconnect", "CLOSE"),
    SERVER_BUSY("SERVER_BUSY", "Server Busy", "Server too busy to handle request", "ERROR"),
    RESOURCE_THROTTLED("RESOURCE_THROTTLED", "Resource Throttled", "Client exceeded resource limits", ""),
    CLIENT_CHANNEL_ERROR("CLIENT_CHANNEL_ERROR", "Client Channel Error", "Client channel error occurred", ""),
    IDLE("IDLE", "Idle Timeout", "Client disconnected due to inactivity", ""),
    INBOX_TRANSIENT_ERROR("INBOX_TRANSIENT_ERROR", "Inbox Error", "Transient inbox error occurred", ""),
    INVALID_TOPIC("INVALID_TOPIC", "Invalid Topic", "Invalid topic name", ""),
    MALFORMED_TOPIC("MALFORMED_TOPIC", "Malformed Topic", "Topic has malformed UTF-8", ""),
    INVALID_TOPIC_FILTER("INVALID_TOPIC_FILTER", "Invalid Topic Filter", "Invalid topic filter", ""),
    MALFORMED_TOPIC_FILTER("MALFORMED_TOPIC_FILTER", "Malformed Topic Filter", "Topic filter has malformed UTF-8", ""),
    KICKED("KICKED", "Kicked", "Client was kicked by admin", "CLOSE"),
    RE_AUTH_FAILED("RE_AUTH_FAILED", "Re-auth Failed", "Client re-authentication failed", ""),
    NO_PUB_PERMISSION("NO_PUB_PERMISSION", "No Publish Permission", "Client lacks publish permission", ""),
    PROTOCOL_VIOLATION("PROTOCOL_VIOLATION", "Protocol Violation", "Client violated protocol rules", ""),
    EXCEED_RECEIVING_LIMIT("EXCEED_RECEIVING_LIMIT", "Exceed Receive Limit", "Client exceeded receiving limit", ""),
    EXCEED_PUB_RATE("EXCEED_PUB_RATE", "Exceed Publish Rate", "Client exceeded publish rate limit", ""),
    TOO_LARGE_SUBSCRIPTION("TOO_LARGE_SUBSCRIPTION", "Too Large Subscription", "Subscription request too large", ""),
    TOO_LARGE_UNSUBSCRIPTION("TOO_LARGE_UNSUBSCRIPTION", "Too Large Unsubscription", "Unsubscription request too large", ""),
    OVERSIZE_PACKET_DROPPED("OVERSIZE_PACKET_DROPPED", "Oversize Packet Dropped", "Oversized packet was dropped", ""),

    // Message events
    PING_REQ("PING_REQ", "Ping Request", "Client sent PINGREQ", "PING"),
    DISCARD("DISCARD", "Discard", "Message was discarded", ""),
    WILL_DISTED("WILL_DISTED", "Will Distributed", "Will message was distributed", ""),
    WILL_DIST_ERROR("WILL_DIST_ERROR", "Will Dist Error", "Error distributing will message", ""),
    QOS0_DIST_ERROR("QOS0_DIST_ERROR", "QoS0 Dist Error", "Error distributing QoS0 message", ""),
    QOS1_DIST_ERROR("QOS1_DIST_ERROR", "QoS1 Dist Error", "Error distributing QoS1 message", ""),
    QOS2_DIST_ERROR("QOS2_DIST_ERROR", "QoS2 Dist Error", "Error distributing QoS2 message", ""),

    // QoS events
    PUB_ACKED("PUB_ACKED", "Pub Acked", "Publish acknowledged (QoS1)", ""),
    PUB_ACK_DROPPED("PUB_ACK_DROPPED", "Pub Ack Dropped", "Publish ack dropped (QoS1)", ""),
    PUB_RECED("PUB_RECED", "Pub Received", "Publish received (QoS2)", ""),
    PUB_REC_DROPPED("PUB_REC_DROPPED", "Pub Rec Dropped", "Publish received dropped (QoS2)", ""),
    MSG_RETAINED("MSG_RETAINED", "Message Retained", "Message retained successfully", ""),
    RETAIN_MSG_CLEARED("RETAIN_MSG_CLEARED", "Retained Msg Cleared", "Retained message cleared", ""),
    MSG_RETAINED_ERROR("MSG_RETAINED_ERROR", "Retain Error", "Error retaining message", ""),
    MATCH_RETAIN_ERROR("MATCH_RETAIN_ERROR", "Match Retain Error", "Error matching retained messages", ""),
    QOS0_PUSHED("QOS0_PUSHED", "QoS0 Pushed", "QoS0 message pushed to client", ""),
    QOS0_DROPPED("QOS0_DROPPED", "QoS0 Dropped", "QoS0 message dropped", ""),
    QOS1_PUSHED("QOS1_PUSHED", "QoS1 Pushed", "QoS1 message pushed to client", ""),
    QOS1_DROPPED("QOS1_DROPPED", "QoS1 Dropped", "QoS1 message dropped", ""),
    QOS1_CONFIRMED("QOS1_CONFIRMED", "QoS1 Confirmed", "QoS1 message confirmed", ""),
    QOS2_PUSHED("QOS2_PUSHED", "QoS2 Pushed", "QoS2 message pushed to client", ""),
    QOS2_RECEIVED("QOS2_RECEIVED", "QoS2 Received", "QoS2 message received by client", ""),
    QOS2_DROPPED("QOS2_DROPPED", "QoS2 Dropped", "QoS2 message dropped", ""),
    QOS2_CONFIRMED("QOS2_CONFIRMED", "QoS2 Confirmed", "QoS2 message confirmed", ""),
    PUB_ACTION_DISALLOW("PUB_ACTION_DISALLOW", "Publish Disallowed", "Publish action not allowed", ""),
    SUB_ACTION_DISALLOW("SUB_ACTION_DISALLOW", "Subscribe Disallowed", "Subscribe action not allowed", ""),
    UNSUB_ACTION_DISALLOW("UNSUB_ACTION_DISALLOW", "Unsubscribe Disallowed", "Unsubscribe action not allowed", ""),
    ACCESS_CONTROL_ERROR("ACCESS_CONTROL_ERROR", "Access Control Error", "Access control check failed", ""),
    SUB_ACKED("SUB_ACKED", "Sub Acked", "Subscribe acknowledged", "SUBSCRIBE"),
    UNSUB_ACKED("UNSUB_ACKED", "Unsub Acked", "Unsubscribe acknowledged", "UNSUBSCRIBE"),

    // Distribution service events
    DISTED("DISTED", "Distributed", "Message distributed successfully", "PUBLISH"),
    DIST_ERROR("DIST_ERROR", "Dist Error", "Error distributing message", "ERROR"),
    DELIVER_ERROR("DELIVER_ERROR", "Deliver Error", "Error delivering message", ""),
    DELIVER_NO_INBOX("DELIVER_NO_INBOX", "No Inbox", "No inbox available for delivery", ""),
    DELIVERED("DELIVERED", "Delivered", "Message delivered successfully", ""),
    MATCHED("MATCHED", "Matched", "Subscription matched successfully", ""),
    MATCH_ERROR("MATCH_ERROR", "Match Error", "Error matching subscription", ""),
    UNMATCHED("UNMATCHED", "Unmatched", "Subscription unmatched successfully", ""),
    UNMATCH_ERROR("UNMATCH_ERROR", "Unmatch Error", "Error unmatching subscription", ""),
    SUBSCRIBED("SUBSCRIBED", "Subscribed", "Client subscribed successfully", ""),
    SUBSCRIBE_ERROR("SUBSCRIBE_ERROR", "Subscribe Error", "Error during subscription", ""),
    UNSUBSCRIBED("UNSUBSCRIBED", "Unsubscribed", "Client unsubscribed successfully", ""),
    UNSUBSCRIBED_ERROR("UNSUBSCRIBED_ERROR", "Unsubscribe Error", "Error during unsubscription", ""),

    // Inbox service events
    OVERFLOWED("OVERFLOWED", "Overflowed", "Inbox overflow occurred", ""),

    // Retain service events
    OUT_OF_TENANT_RESOURCE("OUT_OF_TENANT_RESOURCE", "Out of Resources", "Tenant resource limit exceeded", ""),

    // Session lifecycle events
    MQTT_SESSION_START("MQTT_SESSION_START", "Session Start", "MQTT session started", ""),
    MQTT_SESSION_STOP("MQTT_SESSION_STOP", "Session Stop", "MQTT session stopped", "");

    /**
     * EventType Enum
     */
    private String value;

    private String name;

    private String description;

    /**
     * 业务系统事件类型
     * ThingLinks 业务系统事件类型
     */
    private String businessSystemEventType;

    /**
     * Get EventTypeEnum by value
     */
    public static Optional<EventTypeEnum> byValue(String value) {
        if (StringUtils.isEmpty(value)) {
            return Optional.empty();
        }
        return Stream.of(values())
                .filter(e -> e.value.equalsIgnoreCase(value))
                .findFirst();
    }
}