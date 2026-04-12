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
#define MAX_NICK 32
#define POOL_SIZE 8

typedef struct {
    uint32_t length;
    uint8_t type;
    char payload[MAX_PAYLOAD];
} Message;

enum {
    MSG_HELLO = 1,
    MSG_WELCOME = 2,
    MSG_TEXT = 3,
    MSG_PING = 4,
    MSG_PONG = 5,
    MSG_BYE = 6,
    MSG_AUTH = 7,
    MSG_PRIVATE = 8,
    MSG_ERROR = 9,
    MSG_SERVER_INFO = 10
};

typedef struct {
    int sock;
    char nickname[MAX_NICK];
    int authenticated;
    char addr[64];
} Client;

static pthread_mutex_t q_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_cv = PTHREAD_COND_INITIALIZER;
static std::queue<int> q;

static pthread_mutex_t cli_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<Client> clients;

static void log_recv(uint8_t type) {
    std::cout << "[Layer 4 - Transport] recv()" << std::endl;
    std::cout << "[Layer 6 - Presentation] deserialize Message" << std::endl;
    std::cout << "[Layer 7 - Application] handle type " << (int)type << std::endl;
}

static void log_send(uint8_t type) {
    std::cout << "[Layer 7 - Application] prepare response type " << (int)type << std::endl;
    std::cout << "[Layer 6 - Presentation] serialize Message" << std::endl;
    std::cout << "[Layer 4 - Transport] send()" << std::endl;
}

int recv_all(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = recv(fd, (char *)buf + done, n - done, 0);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

int send_all(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t s = send(fd, (const char *)buf + done, n - done, 0);
        if (s <= 0) return -1;
        done += (size_t)s;
    }
    return 0;
}

int send_msg(int fd, uint8_t type, const char *data, uint32_t dlen) {
    uint32_t length = 1 + dlen;
    uint32_t nl = htonl(length);
    log_send(type);
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
    if (plen > 0 && recv_all(fd, msg->payload, plen) < 0) return -1;
    msg->payload[plen] = '\0';
    log_recv(msg->type);
    return 0;
}

static int find_idx_by_fd(int fd) {
    for (size_t i = 0; i < clients.size(); i++)
        if (clients[i].sock == fd) return (int)i;
    return -1;
}

static int find_idx_by_nick(const std::string &nick) {
    for (size_t i = 0; i < clients.size(); i++)
        if (std::string(clients[i].nickname) == nick) return (int)i;
    return -1;
}

static void remove_client_fd(int fd) {
    pthread_mutex_lock(&cli_mu);
    int idx = find_idx_by_fd(fd);
    if (idx >= 0) {
        std::string nick = clients[(size_t)idx].nickname;
        clients.erase(clients.begin() + idx);
        std::cout << "User [" << nick << "] disconnected" << std::endl;
    }
    pthread_mutex_unlock(&cli_mu);
    close(fd);
}

static void broadcast_text(const std::string &line) {
    std::vector<int> dead;
    pthread_mutex_lock(&cli_mu);
    for (auto &c : clients) {
        if (!c.authenticated) continue;
        if (send_msg(c.sock, MSG_TEXT, line.c_str(), (uint32_t)line.size()) < 0)
            dead.push_back(c.sock);
    }
    pthread_mutex_unlock(&cli_mu);
    for (int fd : dead) remove_client_fd(fd);
}

static void send_server_info(int fd, const std::string &text) {
    send_msg(fd, MSG_SERVER_INFO, text.c_str(), (uint32_t)text.size());
}

static void handle_private(int sender_fd, const std::string &sender_nick, const std::string &payload) {
    size_t pos = payload.find(':');
    if (pos == std::string::npos || pos == 0 || pos + 1 >= payload.size()) {
        send_msg(sender_fd, MSG_ERROR, "bad private format", 18);
        return;
    }
    std::string target = payload.substr(0, pos);
    std::string text = payload.substr(pos + 1);

    int target_fd = -1;
    pthread_mutex_lock(&cli_mu);
    int idx = find_idx_by_nick(target);
    if (idx >= 0 && clients[(size_t)idx].authenticated)
        target_fd = clients[(size_t)idx].sock;
    pthread_mutex_unlock(&cli_mu);

    if (target_fd < 0) {
        send_msg(sender_fd, MSG_ERROR, "target not found", 16);
        return;
    }

    std::string out = "[PRIVATE][" + sender_nick + "]: " + text;
    if (send_msg(target_fd, MSG_PRIVATE, out.c_str(), (uint32_t)out.size()) < 0)
        remove_client_fd(target_fd);
}

static void handle_session(int fd) {
    Message msg{};
    if (recv_msg(fd, &msg) < 0 || msg.type != MSG_HELLO) {
        close(fd);
        return;
    }
    if (send_msg(fd, MSG_WELCOME, "ready for auth", 14) < 0) {
        close(fd);
        return;
    }

    if (recv_msg(fd, &msg) < 0 || msg.type != MSG_AUTH) {
        send_msg(fd, MSG_ERROR, "expected auth", 13);
        close(fd);
        return;
    }

    std::string nick(msg.payload);
    if (nick.empty() || nick.size() >= MAX_NICK) {
        send_msg(fd, MSG_ERROR, "bad nickname", 12);
        close(fd);
        return;
    }

    Client me{};
    me.sock = fd;
    me.authenticated = 1;
    strncpy(me.nickname, nick.c_str(), MAX_NICK - 1);
    strcpy(me.addr, "unknown");

    pthread_mutex_lock(&cli_mu);
    if (find_idx_by_nick(nick) >= 0) {
        pthread_mutex_unlock(&cli_mu);
        send_msg(fd, MSG_ERROR, "nickname already used", 21);
        close(fd);
        return;
    }
    clients.push_back(me);
    pthread_mutex_unlock(&cli_mu);

    std::cout << "[Layer 5 - Session] authentication success" << std::endl;
    std::cout << "User [" << nick << "] connected" << std::endl;
    send_server_info(fd, "auth ok");

    while (true) {
        memset(&msg, 0, sizeof(msg));
        if (recv_msg(fd, &msg) < 0) {
            remove_client_fd(fd);
            return;
        }

        if (msg.type == MSG_TEXT) {
            std::string line = "[" + nick + "]: " + std::string(msg.payload);
            broadcast_text(line);
        } else if (msg.type == MSG_PRIVATE) {
            handle_private(fd, nick, msg.payload);
        } else if (msg.type == MSG_PING) {
            if (send_msg(fd, MSG_PONG, nullptr, 0) < 0) {
                remove_client_fd(fd);
                return;
            }
        } else if (msg.type == MSG_BYE) {
            remove_client_fd(fd);
            return;
        } else {
            send_msg(fd, MSG_ERROR, "unsupported type", 16);
        }
    }
}

static void *worker(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&q_mu);
        while (q.empty()) pthread_cond_wait(&q_cv, &q_mu);
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

    if (bind(sd, (sockaddr *)&saddr, sizeof(saddr)) < 0) {
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

    std::cout << "TCP server (lab4) port 9090, pool " << POOL_SIZE << std::endl;

    for (;;) {
        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int cd = accept(sd, (sockaddr *)&caddr, &clen);
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
