#include <iostream>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <atomic>
#include <pthread.h>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#define MAX_NAME 32
#define MAX_PAYLOAD 256
#define MAX_RETRIES 3
#define ACK_TIMEOUT_MS 2000
#define PING_TIMEOUT_MS 1500

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
    MSG_HELP = 14,
    MSG_ACK = 15
};

typedef struct {
    MessageEx msg;
    int64_t send_time_ms;
    int retries;
    bool acked;
} PendingMsg;

typedef struct {
    uint32_t msg_id;
    int64_t send_time_ms;
    int64_t rtt_ms;
    bool answered;
} PingEntry;

static std::atomic<bool> g_run{true};
static pthread_mutex_t print_mu = PTHREAD_MUTEX_INITIALIZER;
static std::string g_nick;
static int g_sd = -1;
static std::atomic<uint32_t> g_cli_id{1};

static pthread_mutex_t pend_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<PendingMsg> pending;

static pthread_mutex_t ping_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<PingEntry> pings;

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

static int64_t now_ms() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
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

static bool needs_ack(uint8_t t) {
    return t == MSG_TEXT || t == MSG_PRIVATE || t == MSG_PING;
}

// кладём копию сообщения в очередь ожидающих ACK; ретраер потом её заберёт
static void track_pending(const MessageEx &m) {
    PendingMsg p{};
    p.msg = m;
    p.send_time_ms = now_ms();
    p.retries = 0;
    p.acked = false;
    pthread_mutex_lock(&pend_mu);
    pending.push_back(p);
    pthread_mutex_unlock(&pend_mu);
}

static void mark_ack(uint32_t id) {
    pthread_mutex_lock(&pend_mu);
    for (size_t i = 0; i < pending.size(); i++) {
        if (pending[i].msg.msg_id == id) {
            pending[i].acked = true;
            std::cout << "[Transport][RETRY] ACK received (id=" << id << ")" << std::endl;
            break;
        }
    }
    pthread_mutex_unlock(&pend_mu);
}

static void *retry_thread(void *arg) {
    (void)arg;
    while (g_run.load()) {
        usleep(200 * 1000);
        int64_t t = now_ms();
        pthread_mutex_lock(&pend_mu);
        for (size_t i = 0; i < pending.size();) {
            PendingMsg &p = pending[i];
            if (p.acked) {
                pending.erase(pending.begin() + (long)i);
                continue;
            }
            if (t - p.send_time_ms >= ACK_TIMEOUT_MS) {
                if (p.retries >= MAX_RETRIES) {
                    std::cout << "[Transport][RETRY] giving up (id=" << p.msg.msg_id
                              << ") — message undelivered" << std::endl;
                    pending.erase(pending.begin() + (long)i);
                    continue;
                }
                p.retries++;
                p.send_time_ms = t;
                std::cout << "[Transport][RETRY] wait ACK timeout" << std::endl;
                std::cout << "[Transport][RETRY] resend " << p.retries << "/"
                          << MAX_RETRIES << " (id=" << p.msg.msg_id << ")" << std::endl;
                send_msgex(g_sd, p.msg);
            }
            i++;
        }
        pthread_mutex_unlock(&pend_mu);
    }
    return nullptr;
}

static void *recv_thread(void *arg) {
    int fd = *(int *)arg;
    while (g_run.load()) {
        MessageEx m{};
        if (recv_msgex(fd, &m) < 0) {
            pthread_mutex_lock(&print_mu);
            std::cout << "Disconnected" << std::endl;
            pthread_mutex_unlock(&print_mu);
            g_run.store(false);
            break;
        }

        if (m.type == MSG_ACK) {
            mark_ack(m.msg_id);
            continue;
        }

        if (m.type == MSG_PONG) {
            int64_t t = now_ms();
            pthread_mutex_lock(&ping_mu);
            for (auto &pe : pings) {
                if (pe.msg_id == m.msg_id && !pe.answered) {
                    pe.answered = true;
                    pe.rtt_ms = t - pe.send_time_ms;
                    break;
                }
            }
            pthread_mutex_unlock(&ping_mu);
            // PONG тоже подтверждает соответствующий PING — снимаем его с retry-очереди
            mark_ack(m.msg_id);
            continue;
        }

        pthread_mutex_lock(&print_mu);
        if (m.type == MSG_TEXT || m.type == MSG_PRIVATE || m.type == MSG_HISTORY_DATA)
            std::cout << m.payload << std::endl;
        else if (m.type == MSG_SERVER_INFO)
            std::cout << "[SERVER]: " << m.payload << std::endl;
        else if (m.type == MSG_ERROR)
            std::cout << "[ERROR]: " << m.payload << std::endl;
        pthread_mutex_unlock(&print_mu);
    }
    return nullptr;
}

static void send_tracked(MessageEx &m) {
    m.msg_id = g_cli_id.fetch_add(1);
    m.timestamp = (int64_t)time(nullptr);
    strncpy(m.sender, g_nick.c_str(), MAX_NAME - 1);
    if (needs_ack(m.type)) track_pending(m);
    send_msgex(g_sd, m);
}

static void do_ping(int n) {
    if (n <= 0) n = 10;
    pthread_mutex_lock(&ping_mu);
    pings.clear();
    pthread_mutex_unlock(&ping_mu);

    int64_t prev_rtt = -1;
    for (int i = 0; i < n; i++) {
        MessageEx m{};
        m.type = MSG_PING;
        m.msg_id = g_cli_id.fetch_add(1);
        m.timestamp = (int64_t)time(nullptr);
        strncpy(m.sender, g_nick.c_str(), MAX_NAME - 1);
        m.length = 0;

        PingEntry pe{};
        pe.msg_id = m.msg_id;
        pe.send_time_ms = now_ms();
        pe.rtt_ms = -1;
        pe.answered = false;

        pthread_mutex_lock(&ping_mu);
        pings.push_back(pe);
        pthread_mutex_unlock(&ping_mu);

        // PING тоже трекается ретраером, чтобы потери компенсировались
        track_pending(m);
        send_msgex(g_sd, m);

        // ждём ответ до PING_TIMEOUT_MS, выходим раньше при получении
        int64_t deadline = now_ms() + PING_TIMEOUT_MS;
        bool ok = false;
        int64_t rtt = -1;
        while (now_ms() < deadline) {
            usleep(20 * 1000);
            pthread_mutex_lock(&ping_mu);
            for (auto &x : pings) {
                if (x.msg_id == pe.msg_id && x.answered) {
                    ok = true;
                    rtt = x.rtt_ms;
                    break;
                }
            }
            pthread_mutex_unlock(&ping_mu);
            if (ok) break;
        }

        pthread_mutex_lock(&print_mu);
        if (!ok) {
            std::cout << "PING " << (i + 1) << " -> timeout" << std::endl;
            prev_rtt = -1;
        } else if (prev_rtt < 0) {
            std::cout << "PING " << (i + 1) << " -> RTT=" << rtt << "ms" << std::endl;
            prev_rtt = rtt;
        } else {
            int64_t j = rtt - prev_rtt;
            if (j < 0) j = -j;
            std::cout << "PING " << (i + 1) << " -> RTT=" << rtt << "ms | Jitter=" << j << "ms" << std::endl;
            prev_rtt = rtt;
        }
        pthread_mutex_unlock(&print_mu);

        usleep(100 * 1000);
    }
}

static void do_netdiag() {
    pthread_mutex_lock(&ping_mu);
    std::vector<PingEntry> snap = pings;
    pthread_mutex_unlock(&ping_mu);

    if (snap.empty()) {
        std::cout << "no ping data — run /ping first" << std::endl;
        return;
    }

    int sent = (int)snap.size();
    int recvd = 0;
    int64_t rtt_sum = 0;
    int64_t prev = -1;
    int64_t jit_sum = 0;
    int jit_cnt = 0;
    for (auto &p : snap) {
        if (p.answered) {
            recvd++;
            rtt_sum += p.rtt_ms;
            if (prev >= 0) {
                int64_t j = p.rtt_ms - prev;
                if (j < 0) j = -j;
                jit_sum += j;
                jit_cnt++;
            }
            prev = p.rtt_ms;
        } else {
            // потеря рвёт цепочку джиттера — следующая пара берётся заново
            prev = -1;
        }
    }

    double rtt_avg = recvd > 0 ? (double)rtt_sum / recvd : 0.0;
    double jit_avg = jit_cnt > 0 ? (double)jit_sum / jit_cnt : 0.0;
    double loss = sent > 0 ? (double)(sent - recvd) / sent * 100.0 : 0.0;

    std::cout << "RTT avg : " << rtt_avg << " ms" << std::endl;
    std::cout << "Jitter  : " << jit_avg << " ms" << std::endl;
    std::cout << "Loss    : " << loss << " %" << std::endl;

    std::string fname = "net_diag_" + g_nick + ".json";
    std::ofstream f(fname);
    f << "{\n"
      << "  \"nickname\": \"" << g_nick << "\",\n"
      << "  \"sent\": " << sent << ",\n"
      << "  \"received\": " << recvd << ",\n"
      << "  \"rtt_avg_ms\": " << rtt_avg << ",\n"
      << "  \"jitter_avg_ms\": " << jit_avg << ",\n"
      << "  \"loss_percent\": " << loss << "\n"
      << "}\n";
    f.close();
}

static void print_help() {
    std::cout << "Available commands:\n"
              << "/help\n/list\n/history\n/history N\n/quit\n/w <nick> <message>\n"
              << "/ping\n/ping N\n/netdiag\n"
              << "Tip: packets sometimes lie about being delivered\n";
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
    g_sd = sd;

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

    pthread_t rt;
    if (pthread_create(&rt, nullptr, recv_thread, &g_sd) != 0) {
        perror("pthread_create");
        close(sd);
        return 1;
    }
    pthread_t rtr;
    if (pthread_create(&rtr, nullptr, retry_thread, nullptr) != 0) {
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

        if (line == "/help") {
            print_help();
            continue;
        } else if (line == "/netdiag") {
            do_netdiag();
            continue;
        } else if (line == "/ping") {
            do_ping(10);
            continue;
        } else if (line.rfind("/ping ", 0) == 0) {
            int n = atoi(line.substr(6).c_str());
            do_ping(n);
            continue;
        } else if (line == "/list") {
            memset(&m, 0, sizeof(m));
            m.type = MSG_LIST;
            send_tracked(m);
            continue;
        } else if (line == "/history") {
            memset(&m, 0, sizeof(m));
            m.type = MSG_HISTORY;
            send_tracked(m);
            continue;
        } else if (line.rfind("/history ", 0) == 0) {
            memset(&m, 0, sizeof(m));
            m.type = MSG_HISTORY;
            strncpy(m.payload, line.substr(9).c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
            send_tracked(m);
            continue;
        } else if (line == "/quit") {
            memset(&m, 0, sizeof(m));
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
            memset(&m, 0, sizeof(m));
            m.type = MSG_PRIVATE;
            strncpy(m.receiver, target.c_str(), MAX_NAME - 1);
            strncpy(m.payload, text.c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
            send_tracked(m);
        } else {
            memset(&m, 0, sizeof(m));
            m.type = MSG_TEXT;
            strncpy(m.payload, line.c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
            send_tracked(m);
        }
    }

    shutdown(sd, SHUT_RDWR);
    close(sd);
    pthread_join(rt, nullptr);
    pthread_join(rtr, nullptr);
    return 0;
}
