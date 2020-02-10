FROM alpine:3.10

RUN apk add --no-cache --update python3 && rm -rf /var/cache/apk/*


# Define working directory.
WORKDIR /home/
COPY ./proj1.py ./

# Define default command.
CMD ["python3", "-u", "./proj1.py"]
