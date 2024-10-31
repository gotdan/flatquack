FROM oven/bun:latest

WORKDIR /app

COPY . .

RUN bun install

RUN bunx flatquack

CMD ["/bin/bash"]