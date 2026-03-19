#include <iostream>
#include <cstring>
#include <string>
#include <atomic>
#include <pthread.h>
#include <csignal>
#include <unistd.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>

#define MAX_PAYLOAD 1024
#define RECONNECT_SEC 2

typedef struct {
    uint32_t length;
    uint8_t  type;
    char     payload[MAX_PAYLOAD];
} Message;

enum {
    MSG_HELLO   = 1,
    MSG_WELCOME = 2,
    MSG_TEXT    = 3,
    MSG_PING    = 4,
    MSG_PONG    = 5,
    MSG_BYE     = 6
};

static std::atomic<int> g_sd{-1};
static std::atomic<bool> g_run{true};
static pthread_mutex_t print_mu = PTHREAD_MUTEX_INITIALIZER;

int recv_all(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = recv(fd, (char*)buf + done, n - done, 0);
        if (r <= 0) return -1;
        done += r;
    }
    return 0;
}

int send_all(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t s = send(fd, (const char*)buf + done, n - done, 0);
        if (s <= 0) return -1;
        done += s;
    }
    return 0;
}

int send_msg(int fd, uint8_t type, const char *data, uint32_t dlen) {
    uint32_t length = 1 + dlen;
    uint32_t nl = htonl(length);
    if (send_all(fd, &nl, 4) < 0) return -1;
    if (send_all(fd, &type, 1) < 0) return -1;
    if (dlen > 0 && send_all(fd, data, dlen) < 0) return -1;
    return 0;
}

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

static void *recv_thread(void *arg) {
    const char *nick = (const char *)arg;
    (void)nick;

    while (g_run.load()) {
        int fd = g_sd.load();
        if (fd < 0) {
            usleep(100000);
            continue;
        }
        Message msg{};
        if (recv_msg(fd, &msg) < 0) {
            if (!g_run.load())
                break;
            int expected = fd;
            if (g_sd.compare_exchange_strong(expected, -1)) {
                shutdown(fd, SHUT_RDWR);
                close(fd);
                pthread_mutex_lock(&print_mu);
                std::cout << "(connection lost, reconnecting...)" << std::endl;
                pthread_mutex_unlock(&print_mu);
            }
            continue;
        }
        if (msg.type == MSG_TEXT) {
            pthread_mutex_lock(&print_mu);
            std::cout << msg.payload << std::endl;
            pthread_mutex_unlock(&print_mu);
        } else if (msg.type == MSG_PONG) {
            pthread_mutex_lock(&print_mu);
            std::cout << "PONG" << std::endl;
            pthread_mutex_unlock(&print_mu);
        } else if (msg.type == MSG_BYE) {
            pthread_mutex_lock(&print_mu);
            std::cout << "Disconnected" << std::endl;
            pthread_mutex_unlock(&print_mu);
            g_run.store(false);
            break;
        }
    }
    return nullptr;
}

static bool do_connect(const char *host, const char *nick) {
    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
        perror("socket");
        return false;
    }
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(9090);
    if (inet_pton(AF_INET, host, &dest.sin_addr) != 1) {
        close(sd);
        return false;
    }
    if (connect(sd, (sockaddr*)&dest, sizeof(dest)) < 0) {
        perror("connect");
        close(sd);
        return false;
    }

    if (send_msg(sd, MSG_HELLO, nick, (uint32_t)strlen(nick)) < 0) {
        close(sd);
        return false;
    }
    Message msg{};
    if (recv_msg(sd, &msg) < 0 || msg.type != MSG_WELCOME) {
        close(sd);
        return false;
    }

    int old = g_sd.exchange(sd);
    if (old >= 0) {
        shutdown(old, SHUT_RDWR);
        close(old);
    }

    pthread_mutex_lock(&print_mu);
    std::cout << "Welcome " << msg.payload << std::endl;
    pthread_mutex_unlock(&print_mu);
    return true;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    const char *nick = "User";
    const char *host = "127.0.0.1";
    if (argc >= 2) nick = argv[1];
    if (argc >= 3) host = argv[2];

    pthread_t rt;
    if (pthread_create(&rt, nullptr, recv_thread, (void*)nick) != 0) {
        perror("pthread_create");
        return 1;
    }

    while (g_run.load()) {
        while (g_sd.load() < 0 && g_run.load()) {
            if (!do_connect(host, nick)) {
                std::cerr << "retry in " << RECONNECT_SEC << " sec" << std::endl;
                sleep(RECONNECT_SEC);
            }
        }
        if (!g_run.load()) break;

        for (;;) {
            if (g_sd.load() < 0)
                break;
            struct pollfd p{};
            p.fd = STDIN_FILENO;
            p.events = POLLIN;
            if (poll(&p, 1, 400) <= 0)
                continue;
            break;
        }
        if (g_sd.load() < 0)
            continue;

        std::string line;
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            int fd = g_sd.load();
            if (fd >= 0)
                send_msg(fd, MSG_BYE, nullptr, 0);
            g_run.store(false);
            break;
        }
        if (line.empty())
            continue;

        int fd = g_sd.load();
        if (fd < 0)
            continue;

        if (line == "/ping") {
            if (send_msg(fd, MSG_PING, nullptr, 0) < 0) {
                shutdown(fd, SHUT_RDWR);
                close(fd);
                g_sd.store(-1);
            }
        } else if (line == "/quit") {
            send_msg(fd, MSG_BYE, nullptr, 0);
            g_run.store(false);
            break;
        } else {
            uint32_t plen = (uint32_t)line.size();
            if (plen > MAX_PAYLOAD - 1) plen = MAX_PAYLOAD - 1;
            if (send_msg(fd, MSG_TEXT, line.c_str(), plen) < 0) {
                shutdown(fd, SHUT_RDWR);
                close(fd);
                g_sd.store(-1);
            }
        }
    }

    int last = g_sd.load();
    if (last >= 0) {
        shutdown(last, SHUT_RDWR);
        close(last);
        g_sd.store(-1);
    }
    pthread_join(rt, nullptr);
    return 0;
}
