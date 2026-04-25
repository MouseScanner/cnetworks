#include <iostream>
#include <cstring>
#include <string>
#include <queue>
#include <vector>
#include <fstream>
#include <sstream>
#include <atomic>
#include <csignal>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#define MAX_NAME 32
#define MAX_PAYLOAD 256
#define POOL_SIZE 8
#define LAST_IDS 32

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
    int sock;
    char nickname[MAX_NAME];
    uint32_t last_ids[LAST_IDS];
    int last_pos;
} Client;

typedef struct {
    char sender[MAX_NAME];
    char receiver[MAX_NAME];
    char text[MAX_PAYLOAD];
    int64_t timestamp;
    uint32_t msg_id;
} OfflineMsg;

static pthread_mutex_t q_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_cv = PTHREAD_COND_INITIALIZER;
static std::queue<int> q;

static pthread_mutex_t cli_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<Client> clients;

static pthread_mutex_t off_mu = PTHREAD_MUTEX_INITIALIZER;
static std::vector<OfflineMsg> offline_msgs;

static pthread_mutex_t hist_mu = PTHREAD_MUTEX_INITIALIZER;
static std::atomic<uint32_t> g_msg_id{1};
static const char *HISTORY_FILE = "history.jsonl";

static int g_delay_ms = 0;
static double g_drop = 0.0;
static double g_corrupt = 0.0;

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

static void log_in(const MessageEx &m, const char *src) {
    std::cout << "[Application] deserialize MessageEx -> type " << (int)m.type << std::endl;
    std::cout << "[Transport] recv() via TCP" << std::endl;
    std::cout << "[Internet] src=" << src << " dst=127.0.0.1 proto=TCP" << std::endl;
    std::cout << "[Network Access] frame received via network interface" << std::endl;
}

static void log_out(uint8_t type) {
    std::cout << "[Application] prepare type " << (int)type << std::endl;
    std::cout << "[Transport] send() via TCP" << std::endl;
    std::cout << "[Internet] destination ip = 127.0.0.1" << std::endl;
    std::cout << "[Network Access] frame sent to network interface" << std::endl;
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
    log_out(m.type);
    return send_all(fd, &m, sizeof(m));
}

int recv_msgex(int fd, MessageEx *m, const char *src) {
    if (recv_all(fd, m, sizeof(*m)) < 0) return -1;
    m->length = ntohl(m->length);
    m->msg_id = ntohl(m->msg_id);
    uint64_t ts;
    memcpy(&ts, &m->timestamp, sizeof(ts));
    ts = from_net64(ts);
    m->timestamp = (int64_t)ts;
    m->sender[MAX_NAME - 1] = '\0';
    m->receiver[MAX_NAME - 1] = '\0';
    m->payload[MAX_PAYLOAD - 1] = '\0';
    log_in(*m, src);
    return 0;
}

// возвращает 1 если пакет нужно дропнуть, 0 если пропустить дальше
static int sim_apply(MessageEx *m) {
    if (g_delay_ms > 0) {
        std::cout << "[Transport][SIM] DELAY applied: " << g_delay_ms << " ms" << std::endl;
        usleep((useconds_t)g_delay_ms * 1000);
    }
    if (g_drop > 0.0) {
        double r = (double)rand() / (double)RAND_MAX;
        if (r < g_drop) {
            std::cout << "[Transport][SIM] DROP (id=" << m->msg_id << ", rate=" << g_drop << ")" << std::endl;
            return 1;
        }
    }
    if (g_corrupt > 0.0 && m->length > 0) {
        double r = (double)rand() / (double)RAND_MAX;
        if (r < g_corrupt) {
            // меняем один случайный байт в полезной нагрузке (длина ограничена strnlen на случай если payload не \0-терминирован)
            size_t plen = strnlen(m->payload, MAX_PAYLOAD);
            if (plen > 0) {
                size_t pos = (size_t)rand() % plen;
                m->payload[pos] ^= 0x20;
                std::cout << "[Transport][SIM] CORRUPT payload (id=" << m->msg_id << ")" << std::endl;
            }
        }
    }
    return 0;
}

static std::string esc_json(const std::string &s) {
    std::string out;
    for (char c : s) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    return out;
}

static void append_history(const MessageEx &m, const std::string &type, bool delivered, bool is_offline) {
    pthread_mutex_lock(&hist_mu);
    std::ofstream f(HISTORY_FILE, std::ios::app);
    f << "{\"msg_id\":" << m.msg_id
      << ",\"timestamp\":" << m.timestamp
      << ",\"sender\":\"" << esc_json(m.sender) << "\""
      << ",\"receiver\":\"" << esc_json(m.receiver) << "\""
      << ",\"type\":\"" << type << "\""
      << ",\"text\":\"" << esc_json(m.payload) << "\""
      << ",\"delivered\":" << (delivered ? "true" : "false")
      << ",\"is_offline\":" << (is_offline ? "true" : "false")
      << "}\n";
    pthread_mutex_unlock(&hist_mu);
}

static std::string now_fmt(int64_t ts) {
    time_t t = (time_t)ts;
    struct tm tmv{};
    localtime_r(&t, &tmv);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tmv);
    return buf;
}

static void send_server_text(int fd, const std::string &text) {
    MessageEx m{};
    m.type = MSG_SERVER_INFO;
    m.msg_id = g_msg_id.fetch_add(1);
    m.timestamp = (int64_t)time(nullptr);
    strncpy(m.sender, "SERVER", MAX_NAME - 1);
    strncpy(m.payload, text.c_str(), MAX_PAYLOAD - 1);
    m.length = (uint32_t)strlen(m.payload);
    send_msgex(fd, m);
}

static void send_ack(int fd, uint32_t id) {
    MessageEx a{};
    a.type = MSG_ACK;
    a.msg_id = id;
    a.timestamp = (int64_t)time(nullptr);
    strncpy(a.sender, "SERVER", MAX_NAME - 1);
    a.length = 0;
    std::cout << "[Transport][ACK] send MSG_ACK (id=" << id << ")" << std::endl;
    send_msgex(fd, a);
}

static int find_client_idx_by_fd(int fd) {
    for (size_t i = 0; i < clients.size(); i++)
        if (clients[i].sock == fd) return (int)i;
    return -1;
}

static int find_client_fd_by_nick(const std::string &nick) {
    for (auto &c : clients)
        if (nick == c.nickname) return c.sock;
    return -1;
}

// проверка дубликата по последним LAST_IDS обработанным id (кольцевой буфер)
// возвращает 1 если такой id уже видели, 0 если новый и записали
static int check_and_mark_id(int fd, uint32_t id) {
    pthread_mutex_lock(&cli_mu);
    int idx = find_client_idx_by_fd(fd);
    if (idx < 0) {
        pthread_mutex_unlock(&cli_mu);
        return 0;
    }
    for (int i = 0; i < LAST_IDS; i++) {
        if (clients[idx].last_ids[i] == id) {
            pthread_mutex_unlock(&cli_mu);
            return 1;
        }
    }
    clients[idx].last_ids[clients[idx].last_pos] = id;
    clients[idx].last_pos = (clients[idx].last_pos + 1) % LAST_IDS;
    pthread_mutex_unlock(&cli_mu);
    return 0;
}

static void remove_client(int fd) {
    pthread_mutex_lock(&cli_mu);
    for (size_t i = 0; i < clients.size(); i++) {
        if (clients[i].sock == fd) {
            std::cout << "User [" << clients[i].nickname << "] disconnected" << std::endl;
            clients.erase(clients.begin() + (long)i);
            break;
        }
    }
    pthread_mutex_unlock(&cli_mu);
    close(fd);
}

static void deliver_offline_for(const std::string &nick, int fd) {
    pthread_mutex_lock(&off_mu);
    std::vector<size_t> to_remove;
    for (size_t i = 0; i < offline_msgs.size(); i++) {
        if (nick == offline_msgs[i].receiver) {
            MessageEx m{};
            m.type = MSG_PRIVATE;
            m.msg_id = offline_msgs[i].msg_id;
            m.timestamp = offline_msgs[i].timestamp;
            strncpy(m.sender, offline_msgs[i].sender, MAX_NAME - 1);
            strncpy(m.receiver, offline_msgs[i].receiver, MAX_NAME - 1);
            std::string body = "[OFFLINE][" + std::string(m.sender) + " -> " + std::string(m.receiver) + "]: " + offline_msgs[i].text;
            strncpy(m.payload, body.c_str(), MAX_PAYLOAD - 1);
            m.length = (uint32_t)strlen(m.payload);
            send_msgex(fd, m);
            to_remove.push_back(i);
        }
    }
    for (size_t j = to_remove.size(); j > 0; j--)
        offline_msgs.erase(offline_msgs.begin() + (long)to_remove[j - 1]);
    pthread_mutex_unlock(&off_mu);
}

static std::string build_online_list() {
    std::ostringstream oss;
    oss << "Online users:\n";
    pthread_mutex_lock(&cli_mu);
    for (auto &c : clients) oss << c.nickname << "\n";
    pthread_mutex_unlock(&cli_mu);
    return oss.str();
}

static std::string history_tail(int n) {
    pthread_mutex_lock(&hist_mu);
    std::ifstream f(HISTORY_FILE);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(f, line)) lines.push_back(line);
    pthread_mutex_unlock(&hist_mu);
    if (n <= 0) n = 20;
    int start = (int)lines.size() - n;
    if (start < 0) start = 0;
    std::ostringstream oss;
    for (int i = start; i < (int)lines.size(); i++) oss << lines[(size_t)i] << "\n";
    return oss.str();
}

static void handle_session(int fd) {
    sockaddr_in peer{};
    socklen_t plen = sizeof(peer);
    char src[64] = "unknown";
    if (getpeername(fd, (sockaddr *)&peer, &plen) == 0) {
        char ipstr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, ipstr, sizeof(ipstr));
        snprintf(src, sizeof(src), "%s:%d", ipstr, ntohs(peer.sin_port));
    }

    MessageEx m{};
    if (recv_msgex(fd, &m, src) < 0 || m.type != MSG_HELLO) {
        close(fd);
        return;
    }
    MessageEx w{};
    w.type = MSG_WELCOME;
    w.msg_id = g_msg_id.fetch_add(1);
    w.timestamp = (int64_t)time(nullptr);
    strncpy(w.sender, "SERVER", MAX_NAME - 1);
    strncpy(w.payload, "ready", MAX_PAYLOAD - 1);
    w.length = (uint32_t)strlen(w.payload);
    if (send_msgex(fd, w) < 0) {
        close(fd);
        return;
    }

    if (recv_msgex(fd, &m, src) < 0 || m.type != MSG_AUTH || strlen(m.payload) == 0) {
        send_server_text(fd, "auth required");
        close(fd);
        return;
    }
    std::string nick = m.payload;

    pthread_mutex_lock(&cli_mu);
    if (find_client_fd_by_nick(nick) >= 0) {
        pthread_mutex_unlock(&cli_mu);
        send_server_text(fd, "nickname already used");
        close(fd);
        return;
    }
    Client c{};
    c.sock = fd;
    strncpy(c.nickname, nick.c_str(), MAX_NAME - 1);
    c.last_pos = 0;
    for (int i = 0; i < LAST_IDS; i++) c.last_ids[i] = 0;
    clients.push_back(c);
    pthread_mutex_unlock(&cli_mu);

    std::cout << "[Application] authentication success: " << nick << std::endl;
    std::cout << "User [" << nick << "] connected" << std::endl;
    deliver_offline_for(nick, fd);

    while (true) {
        memset(&m, 0, sizeof(m));
        if (recv_msgex(fd, &m, src) < 0) {
            remove_client(fd);
            return;
        }

        // помехи применяются до прикладной обработки.
        // PING обрабатывается отдельной веткой логов как [Transport][PING]
        if (m.type == MSG_PING) {
            std::cout << "[Transport][PING] recv MSG_PING (id=" << m.msg_id << ")" << std::endl;
        }
        if (sim_apply(&m)) {
            // дроп — никаких ответов клиенту, прикладной уровень об этом не знает
            continue;
        }

        if (m.type == MSG_TEXT) {
            uint32_t cli_id = m.msg_id;
            if (check_and_mark_id(fd, cli_id)) {
                std::cout << "[Application][DEDUP] duplicate ignored (id=" << cli_id << ")" << std::endl;
                send_ack(fd, cli_id);
                continue;
            }
            std::cout << "[Application][ACK] process MSG_TEXT (id=" << cli_id << ")" << std::endl;
            uint32_t broadcast_id = g_msg_id.fetch_add(1);
            int64_t ts = (int64_t)time(nullptr);
            std::string line = "[" + now_fmt(ts) + "][id=" + std::to_string(broadcast_id) + "][" + nick + "]: " + std::string(m.payload);

            MessageEx hist{};
            hist.msg_id = broadcast_id;
            hist.timestamp = ts;
            strncpy(hist.sender, nick.c_str(), MAX_NAME - 1);
            strncpy(hist.payload, m.payload, MAX_PAYLOAD - 1);
            append_history(hist, "MSG_TEXT", true, false);

            pthread_mutex_lock(&cli_mu);
            for (auto &cc : clients) {
                MessageEx out{};
                out.type = MSG_TEXT;
                out.msg_id = broadcast_id;
                out.timestamp = ts;
                strncpy(out.sender, nick.c_str(), MAX_NAME - 1);
                strncpy(out.payload, line.c_str(), MAX_PAYLOAD - 1);
                out.length = (uint32_t)strlen(out.payload);
                send_msgex(cc.sock, out);
            }
            pthread_mutex_unlock(&cli_mu);

            send_ack(fd, cli_id);
        } else if (m.type == MSG_PRIVATE) {
            uint32_t cli_id = m.msg_id;
            if (check_and_mark_id(fd, cli_id)) {
                std::cout << "[Application][DEDUP] duplicate ignored (id=" << cli_id << ")" << std::endl;
                send_ack(fd, cli_id);
                continue;
            }
            std::cout << "[Application][ACK] process MSG_PRIVATE (id=" << cli_id << ")" << std::endl;
            uint32_t fwd_id = g_msg_id.fetch_add(1);
            int64_t ts = (int64_t)time(nullptr);

            int tfd = -1;
            pthread_mutex_lock(&cli_mu);
            tfd = find_client_fd_by_nick(m.receiver);
            pthread_mutex_unlock(&cli_mu);

            MessageEx hist{};
            hist.msg_id = fwd_id;
            hist.timestamp = ts;
            strncpy(hist.sender, nick.c_str(), MAX_NAME - 1);
            strncpy(hist.receiver, m.receiver, MAX_NAME - 1);
            strncpy(hist.payload, m.payload, MAX_PAYLOAD - 1);

            if (tfd >= 0) {
                std::string line = "[" + now_fmt(ts) + "][id=" + std::to_string(fwd_id) + "][PRIVATE][" + nick + " -> " + std::string(m.receiver) + "]: " + std::string(m.payload);
                MessageEx out{};
                out.type = MSG_PRIVATE;
                out.msg_id = fwd_id;
                out.timestamp = ts;
                strncpy(out.sender, nick.c_str(), MAX_NAME - 1);
                strncpy(out.receiver, m.receiver, MAX_NAME - 1);
                strncpy(out.payload, line.c_str(), MAX_PAYLOAD - 1);
                out.length = (uint32_t)strlen(out.payload);
                send_msgex(tfd, out);
                append_history(hist, "MSG_PRIVATE", true, false);
            } else {
                OfflineMsg om{};
                strncpy(om.sender, nick.c_str(), MAX_NAME - 1);
                strncpy(om.receiver, m.receiver, MAX_NAME - 1);
                strncpy(om.text, m.payload, MAX_PAYLOAD - 1);
                om.msg_id = fwd_id;
                om.timestamp = ts;
                pthread_mutex_lock(&off_mu);
                offline_msgs.push_back(om);
                pthread_mutex_unlock(&off_mu);
                append_history(hist, "MSG_PRIVATE", false, true);
                send_server_text(fd, "receiver offline, saved");
            }

            send_ack(fd, cli_id);
        } else if (m.type == MSG_LIST) {
            std::string lst = build_online_list();
            send_server_text(fd, lst);
        } else if (m.type == MSG_HISTORY) {
            int n = 20;
            if (strlen(m.payload) > 0) n = atoi(m.payload);
            std::string hist = history_tail(n);
            MessageEx out{};
            out.type = MSG_HISTORY_DATA;
            out.msg_id = g_msg_id.fetch_add(1);
            out.timestamp = (int64_t)time(nullptr);
            strncpy(out.sender, "SERVER", MAX_NAME - 1);
            strncpy(out.payload, hist.c_str(), MAX_PAYLOAD - 1);
            out.length = (uint32_t)strlen(out.payload);
            send_msgex(fd, out);
        } else if (m.type == MSG_PING) {
            // эхо: тот же msg_id обратно, чтобы клиент сопоставил с отправленным запросом
            MessageEx out{};
            out.type = MSG_PONG;
            out.msg_id = m.msg_id;
            out.timestamp = (int64_t)time(nullptr);
            strncpy(out.sender, "SERVER", MAX_NAME - 1);
            out.length = 0;
            std::cout << "[Transport][PING] send MSG_PONG (id=" << m.msg_id << ")" << std::endl;
            send_msgex(fd, out);
        } else if (m.type == MSG_BYE) {
            remove_client(fd);
            return;
        } else if (m.type == MSG_HELP) {
            send_server_text(fd, "use /help locally");
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

static void parse_args(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--delay=", 8) == 0) g_delay_ms = atoi(argv[i] + 8);
        else if (strncmp(argv[i], "--drop=", 7) == 0) g_drop = atof(argv[i] + 7);
        else if (strncmp(argv[i], "--corrupt=", 10) == 0) g_corrupt = atof(argv[i] + 10);
    }
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(nullptr));
    parse_args(argc, argv);

    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }
    int opt = 1;
    setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(9091);
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

    std::cout << "TCP server (lab6) port 9091, pool " << POOL_SIZE
              << ", delay=" << g_delay_ms << "ms drop=" << g_drop << " corrupt=" << g_corrupt << std::endl;
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
