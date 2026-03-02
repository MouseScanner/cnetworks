#include <iostream>
#include <cstring>
#include <csignal>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h>

#define MAX_PAYLOAD 1024

typedef struct {
    uint32_t length;
    uint8_t  type;
    char     payload[MAX_PAYLOAD];
} Message;

enum {
    MSG_HELLO   = 1,
    MSG_WELCOME = 2,
    MSG_TEXT    = 3,
    MSG_PING   = 4,
    MSG_PONG   = 5,
    MSG_BYE    = 6
};

// чтение ровно n байт из сокета
int recv_all(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = recv(fd, (char*)buf + done, n - done, 0);
        if (r <= 0) return -1;
        done += r;
    }
    return 0;
}

// отправка ровно n байт
int send_all(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t s = send(fd, (const char*)buf + done, n - done, 0);
        if (s <= 0) return -1;
        done += s;
    }
    return 0;
}

// отправка сообщения по протоколу
int send_msg(int fd, uint8_t type, const char *data, uint32_t dlen) {
    uint32_t length = 1 + dlen;
    uint32_t nl = htonl(length);
    if (send_all(fd, &nl, 4) < 0) return -1;
    if (send_all(fd, &type, 1) < 0) return -1;
    if (dlen > 0 && send_all(fd, data, dlen) < 0) return -1;
    return 0;
}

// приём сообщения
int recv_msg(int fd, Message *msg) {
    uint32_t nl;
    if (recv_all(fd, &nl, 4) < 0) return -1;
    msg->length = ntohl(nl);
    if (msg->length < 1 || msg->length > MAX_PAYLOAD) return -1;
    if (recv_all(fd, &msg->type, 1) < 0) return -1;
    uint32_t plen = msg->length - 1;
    if (plen > 0) {
        if (recv_all(fd, msg->payload, plen) < 0) return -1;
    }
    msg->payload[plen] = '\0';
    return 0;
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(9090);
    saddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sd, (sockaddr*)&saddr, sizeof(saddr)) < 0) {
        perror("bind");
        close(sd);
        return 1;
    }

    if (listen(sd, 1) < 0) {
        perror("listen");
        close(sd);
        return 1;
    }

    std::cout << "TCP server running on port 9090" << std::endl;

    sockaddr_in caddr{};
    socklen_t clen = sizeof(caddr);
    int cd = accept(sd, (sockaddr*)&caddr, &clen);
    if (cd < 0) {
        perror("accept");
        close(sd);
        return 1;
    }

    char cstr[64];
    snprintf(cstr, sizeof(cstr), "%s:%d",
             inet_ntoa(caddr.sin_addr), ntohs(caddr.sin_port));

    std::cout << "Client connected" << std::endl;

    // ждём hello от клиента
    Message msg{};
    if (recv_msg(cd, &msg) < 0 || msg.type != MSG_HELLO) {
        std::cerr << "no HELLO" << std::endl;
        close(cd);
        close(sd);
        return 1;
    }

    // отправляем welcome
    send_msg(cd, MSG_WELCOME, cstr, strlen(cstr));

    // основной цикл обработки
    while (true) {
        memset(&msg, 0, sizeof(msg));
        if (recv_msg(cd, &msg) < 0) {
            std::cout << "Client disconnected" << std::endl;
            break;
        }

        if (msg.type == MSG_TEXT) {
            std::cout << "[" << cstr << "]: " << msg.payload << std::endl;
        } else if (msg.type == MSG_PING) {
            send_msg(cd, MSG_PONG, nullptr, 0);
        } else if (msg.type == MSG_BYE) {
            std::cout << "Client disconnected" << std::endl;
            break;
        }
    }

    close(cd);
    close(sd);
    return 0;
}
