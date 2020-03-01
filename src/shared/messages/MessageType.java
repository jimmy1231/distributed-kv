package shared.messages;

public enum MessageType {
    ECS_TO_SERVER,      /* ECS to SERVER message */
    SERVER_TO_ECES,     /* SERVER to ECS message */

    CLIENT_TO_SERVER,   /* CLIENT to SERVER message */
    SERVER_TO_CLIENT,   /* SERVER to CLIENT message */

    SERVER_TO_SERVER    /* SERVER to SERVER message */
}
