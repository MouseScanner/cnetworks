#include <iostream>
#include <cstring>
#include <string>
#include <queue>
#include <vector>
#include <csignal>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h>

#define MAX_PAYLOAD 1024
#define POOL_SIZE 10

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

struct Cli {
    int fd;
    std::string nick;
    std::string addr;
};

static pthread_mutex_t q_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_cv = PTHREAD_COND_INITIALIZER;
static std::queue<int> q;

static pthread_mutex_t cli_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<Cli> clients;

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

static void remove_client(int fd) {
    pthread_mutex_lock(&cli_mu);
    for (size_t i = 0; i < clients.size(); i++) {
        if (clients[i].fd == fd) {
            std::cout << "Client disconnected: " << clients[i].nick
                      << " [" << clients[i].addr << "]" << std::endl;
            close(fd);
            clients.erase(clients.begin() + (long)i);
            break;
        }
    }
    pthread_mutex_unlock(&cli_mu);
}

static void broadcast_line(const std::string &line) {
    std::vector<int> dead;
    pthread_mutex_lock(&cli_mu);
    for (auto &c : clients) {
        if (send_msg(c.fd, MSG_TEXT, line.c_str(), (uint32_t)line.size()) < 0)
            dead.push_back(c.fd);
    }
    pthread_mutex_unlock(&cli_mu);
    for (int fd : dead)
        remove_client(fd);
}

static void handle_session(int fd) {
    sockaddr_in peer{};
    socklen_t plen = sizeof(peer);
    char addrbuf[64];
    if (getpeername(fd, (sockaddr*)&peer, &plen) == 0) {
        char ipstr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, ipstr, sizeof(ipstr));
        snprintf(addrbuf, sizeof(addrbuf), "%s:%d", ipstr, ntohs(peer.sin_port));
    } else
        snprintf(addrbuf, sizeof(addrbuf), "?");

    Message msg{};
    if (recv_msg(fd, &msg) < 0 || msg.type != MSG_HELLO) {
        close(fd);
        return;
    }

    std::string nick(msg.payload);
    if (nick.empty()) nick = "anon";

    if (send_msg(fd, MSG_WELCOME, addrbuf, strlen(addrbuf)) < 0) {
        close(fd);
        return;
    }

    Cli me{};
    me.fd = fd;
    me.nick = nick;
    me.addr = addrbuf;

    pthread_mutex_lock(&cli_mu);
    clients.push_back(me);
    pthread_mutex_unlock(&cli_mu);

    while (true) {
        memset(&msg, 0, sizeof(msg));
        if (recv_msg(fd, &msg) < 0) {
            remove_client(fd);
            return;
        }
        if (msg.type == MSG_TEXT) {
            uint32_t tlen = msg.length > 1 ? msg.length - 1 : 0;
            std::string line = nick + " [" + addrbuf + "]: ";
            line.append(msg.payload, tlen);
            broadcast_line(line);
        } else if (msg.type == MSG_PING) {
            if (send_msg(fd, MSG_PONG, nullptr, 0) < 0) {
                remove_client(fd);
                return;
            }
        } else if (msg.type == MSG_BYE) {
            remove_client(fd);
            return;
        }
    }
}

static void *worker(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&q_mu);
        while (q.empty())
            pthread_cond_wait(&q_cv, &q_mu);
        int fd = q.front();
        q.pop();
        pthread_mutex_unlock(&q_mu);
        handle_session(fd);
    }
    return nullptr;
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
    if (listen(sd, 32) < 0) {
        perror("listen");
        close(sd);
        return 1;
    }

    pthread_t th[POOL_SIZE];
    for (int i = 0; i < POOL_SIZE; i++) {
        if (pthread_create(&th[i], nullptr, worker, nullptr) != 0) {
            perror("pthread_create");
            close(sd);
            return 1;
        }
        pthread_detach(th[i]);
    }

    std::cout << "TCP server (lab3) port 9090, pool " << POOL_SIZE << std::endl;

    for (;;) {
        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int cd = accept(sd, (sockaddr*)&caddr, &clen);
        if (cd < 0) {
            perror("accept");
            continue;
        }
        pthread_mutex_lock(&q_mu);
        q.push(cd);
        pthread_cond_signal(&q_cv);
        pthread_mutex_unlock(&q_mu);
    }
}
