#include <iostream>
#include <cstring>
#include <string>
#include <atomic>
#include <pthread.h>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <time.h>

#define MAX_NAME 32
#define MAX_PAYLOAD 256

typedef struct {
    uint32_t length;
    uint8_t type;
    uint32_t msg_id;
    char sender[MAX_NAME];
    char receiver[MAX_NAME];
    int64_t timestamp;
    char payload[MAX_PAYLOAD];
} MessageEx;

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
    MSG_SERVER_INFO = 10,
    MSG_LIST = 11,
    MSG_HISTORY = 12,
    MSG_HISTORY_DATA = 13,
    MSG_HELP = 14
};

static std::atomic<bool> g_run{true};
static pthread_mutex_t print_mu = PTHREAD_MUTEX_INITIALIZER;
static std::string g_nick;

static uint64_t to_net64(uint64_t v) {
    uint32_t hi = htonl((uint32_t)(v >> 32));
    uint32_t lo = htonl((uint32_t)(v & 0xFFFFFFFFULL));
    return ((uint64_t)lo << 32) | hi;
}

static uint64_t from_net64(uint64_t v) {
    uint32_t hi = ntohl((uint32_t)(v >> 32));
    uint32_t lo = ntohl((uint32_t)(v & 0xFFFFFFFFULL));
    return ((uint64_t)lo << 32) | hi;
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

int send_msgex(int fd, MessageEx m) {
    m.length = htonl(m.length);
    m.msg_id = htonl(m.msg_id);
    uint64_t ts = to_net64((uint64_t)m.timestamp);
    memcpy(&m.timestamp, &ts, sizeof(ts));
    return send_all(fd, &m, sizeof(m));
}

int recv_msgex(int fd, MessageEx *m) {
    if (recv_all(fd, m, sizeof(*m)) < 0) return -1;
    m->length = ntohl(m->length);
    m->msg_id = ntohl(m->msg_id);
    uint64_t ts;
    memcpy(&ts, &m->timestamp, sizeof(ts));
    m->timestamp = (int64_t)from_net64(ts);
    m->sender[MAX_NAME - 1] = '\0';
    m->receiver[MAX_NAME - 1] = '\0';
    m->payload[MAX_PAYLOAD - 1] = '\0';
    return 0;
}

typedef struct {
    int fd;
} RecvArgs;

static void *recv_thread(void *arg) {
    int fd = ((RecvArgs *)arg)->fd;
    while (g_run.load()) {
        MessageEx m{};
        if (recv_msgex(fd, &m) < 0) {
            pthread_mutex_lock(&print_mu);
            std::cout << "Disconnected" << std::endl;
            pthread_mutex_unlock(&print_mu);
            g_run.store(false);
            break;
        }

        pthread_mutex_lock(&print_mu);
        if (m.type == MSG_TEXT || m.type == MSG_PRIVATE || m.type == MSG_HISTORY_DATA)
            std::cout << m.payload << std::endl;
        else if (m.type == MSG_PONG)
            std::cout << "[SERVER]: PONG" << std::endl;
        else if (m.type == MSG_SERVER_INFO)
            std::cout << "[SERVER]: " << m.payload << std::endl;
        else if (m.type == MSG_ERROR)
            std::cout << "[ERROR]: " << m.payload << std::endl;
        pthread_mutex_unlock(&print_mu);
    }
    return nullptr;
}

static void print_help() {
    std::cout << "Available commands:\n"
              << "/help\n/list\n/history\n/history N\n/quit\n/w <nick> <message>\n/ping\n"
              << "Tip: packets never sleep\n";
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    const char *host = "127.0.0.1";
    if (argc >= 2) host = argv[1];

    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(9091);
    inet_pton(AF_INET, host, &dest.sin_addr);
    if (connect(sd, (sockaddr *)&dest, sizeof(dest)) < 0) {
        perror("connect");
        close(sd);
        return 1;
    }

    MessageEx m{};
    m.type = MSG_HELLO;
    m.timestamp = (int64_t)time(nullptr);
    strncpy(m.sender, "client", MAX_NAME - 1);
    strncpy(m.payload, "hello", MAX_PAYLOAD - 1);
    m.length = (uint32_t)strlen(m.payload);
    send_msgex(sd, m);
    if (recv_msgex(sd, &m) < 0 || m.type != MSG_WELCOME) {
        close(sd);
        return 1;
    }

    std::cout << "Enter nickname: " << std::flush;
    std::getline(std::cin, g_nick);
    if (g_nick.empty()) g_nick = "User";
    memset(&m, 0, sizeof(m));
    m.type = MSG_AUTH;
    m.timestamp = (int64_t)time(nullptr);
    strncpy(m.sender, g_nick.c_str(), MAX_NAME - 1);
    strncpy(m.payload, g_nick.c_str(), MAX_PAYLOAD - 1);
    m.length = (uint32_t)strlen(m.payload);
    send_msgex(sd, m);

    RecvArgs args{sd};
    pthread_t rt;
    if (pthread_create(&rt, nullptr, recv_thread, &args) != 0) {
        perror("pthread_create");
        close(sd);
        return 1;
    }

    while (g_run.load()) {
        std::string line;
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            memset(&m, 0, sizeof(m));
            m.type = MSG_BYE;
            send_msgex(sd, m);
            break;
        }
        if (line.empty()) continue;

        memset(&m, 0, sizeof(m));
        m.timestamp = (int64_t)time(nullptr);
        strncpy(m.sender, g_nick.c_str(), MAX_NAME - 1);

        if (line == "/help") {
            print_help();
            continue;
        } else if (line == "/list") {
            m.type = MSG_LIST;
        } else if (line == "/history") {
            m.type = MSG_HISTORY;
        } else if (line.rfind("/history ", 0) == 0) {
            m.type = MSG_HISTORY;
            strncpy(m.payload, line.substr(9).c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
        } else if (line == "/ping") {
            m.type = MSG_PING;
        } else if (line == "/quit") {
            m.type = MSG_BYE;
            send_msgex(sd, m);
            g_run.store(false);
            break;
        } else if (line.rfind("/w ", 0) == 0) {
            size_t s1 = line.find(' ', 3);
            if (s1 == std::string::npos) {
                std::cout << "usage: /w <nick> <message>" << std::endl;
                continue;
            }
            std::string target = line.substr(3, s1 - 3);
            std::string text = line.substr(s1 + 1);
            m.type = MSG_PRIVATE;
            strncpy(m.receiver, target.c_str(), MAX_NAME - 1);
            strncpy(m.payload, text.c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
        } else {
            m.type = MSG_TEXT;
            strncpy(m.payload, line.c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
        }

        if (send_msgex(sd, m) < 0) {
            std::cout << "send failed" << std::endl;
            g_run.store(false);
            break;
        }
    }

    shutdown(sd, SHUT_RDWR);
    close(sd);
    pthread_join(rt, nullptr);
    return 0;
}
