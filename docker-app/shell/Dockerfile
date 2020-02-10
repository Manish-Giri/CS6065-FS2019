FROM alpine:3.10

RUN apk add --no-cache --update tzdata && rm -rf /var/cache/apk/*
ENV TZ America/New_York
# Define working directory.
WORKDIR /home/
COPY ./p1.sh ./

# Define default command.
CMD ["sh", "p1.sh"]