#include <iostream>
#include <cstring>
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
    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(9090);
    inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr);

    if (connect(sd, (sockaddr*)&dest, sizeof(dest)) < 0) {
        perror("connect");
        close(sd);
        return 1;
    }

    std::cout << "Connected" << std::endl;

    // отправляем hello с ником
    const char *nick = "User";
    send_msg(sd, MSG_HELLO, nick, strlen(nick));

    // ждём welcome
    Message msg{};
    if (recv_msg(sd, &msg) < 0 || msg.type != MSG_WELCOME) {
        std::cerr << "no WELCOME" << std::endl;
        close(sd);
        return 1;
    }
    std::cout << "Welcome " << msg.payload << std::endl;

    // основной цикл
    std::string line;
    while (true) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line))
            break;
        if (line.empty())
            continue;

        if (line == "/ping") {
            send_msg(sd, MSG_PING, nullptr, 0);
            memset(&msg, 0, sizeof(msg));
            if (recv_msg(sd, &msg) < 0) {
                std::cout << "Disconnected" << std::endl;
                break;
            }
            if (msg.type == MSG_PONG)
                std::cout << "PONG" << std::endl;
            else if (msg.type == MSG_BYE) {
                std::cout << "Disconnected" << std::endl;
                break;
            }
        } else if (line == "/quit") {
            send_msg(sd, MSG_BYE, nullptr, 0);
            std::cout << "Disconnected" << std::endl;
            break;
        } else {
            uint32_t plen = line.size();
            if (plen > MAX_PAYLOAD - 1) plen = MAX_PAYLOAD - 1;
            send_msg(sd, MSG_TEXT, line.c_str(), plen);
        }
    }

    close(sd);
    return 0;
}
