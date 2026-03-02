CC = g++
FLAGS = -std=c++17 -Wall

all: server client

server: server.cpp
	$(CC) $(FLAGS) -o server server.cpp

client: client.cpp
	$(CC) $(FLAGS) -o client client.cpp

clean:
	rm -f server client
