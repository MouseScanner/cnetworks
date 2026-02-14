#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int sd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr);

    std::string line;
    char reply[512];

    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line) || line == "quit")
            break;
        if (line.empty())
            continue;

        sendto(sd, line.c_str(), line.size(), 0, (sockaddr*)&dest, sizeof(dest));

        socklen_t dlen = sizeof(dest);
        int n = recvfrom(sd, reply, sizeof(reply) - 1, 0, (sockaddr*)&dest, &dlen);
        if (n > 0) {
            reply[n] = '\0';
            std::cout << "echo: " << reply << std::endl;
        }
    }

    close(sd);
    return 0;
}
