package com.mqttsnet.thinglinks.enumeration;


import java.util.Optional;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @program: thinglinks
 * @description: 事件枚举
 * @author: mqttsnet
 * @e-mainl: mqttsnet@163.com
 * @date: 2022-12-16 19:42
 **/
@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum EventTypeEnum {

    /**
     * Event type for publish.
     */
    PUBLISH("PUBLISH"),

    /**
     * Event type for write.
     */
    WRITE("WRITE"),

    /**
     * Event type for cluster.
     */
    CLUSTER("CLUSTER"),

    /**
     * Event type for connect.
     */
    CONNECT("CONNECT"),

    /**
     * Event type for close.
     */
    CLOSE("CLOSE"),

    /**
     * Event type for subscribe.
     */
    SUBSCRIBE("SUBSCRIBE"),

    /**
     * Event type for unsubscribe.
     */
    UNSUBSCRIBE("UNSUBSCRIBE"),

    /**
     * Event type for bridge.
     */
    BRIDGE("BRIDGE"),

    /**
     * Event type for disconnect.
     */
    DISCONNECT("DISCONNECT"),

    /**
     * Event type for ping.
     */
    PING("PING"),

    /**
     * Event type for publish acknowledgment.
     */
    PUBLISH_ACK("PUBLISH_ACK"),

    /**
     * Event type for retry.
     */
    RETRY("RETRY"),

    /**
     * Event type for heart timeout.
     */
    HEART_TIMEOUT("HEART_TIMEOUT"),

    /**
     * Event type for system.
     */
    SYSTEM("SYSTEM"),

    /**
     * Event type for error.
     */
    ERROR("ERROR"),

    ;

    private String name;

    /**
     * Retrieves the corresponding EventEnum for a given name.
     *
     * @param name the name of the MQTT event.
     * @return an Optional of EventEnum.
     */
    public static Optional<EventTypeEnum> getEventEnum(String name) {
        if (StringUtils.isEmpty(name)) {
            return Optional.empty();
        }
        return Stream.of(EventTypeEnum.values())
                .filter(event -> event.getName().equalsIgnoreCase(name))
                .findFirst();
    }
}