## Docker App

This is a docker application that provides a listing of all the files in a particular folder along with number of words in each file, and the file with the maximum word count.

The user will map a folder, containing text files, in their computer, to the `/home/data` path in the container created from the included docker image (refer `manishgiri.tar`). The container will thenprint out the aforementioned details on the console, and exit.

The application was first built with Python, using the [`alpine-python`](https://github.com/jfloff/alpine-python) image, which led to an image size of around 60 MB. To minimize the image size further, I then re-built it using plain Shell. Refer the [shell](./shell) folder for the final docker image (compressed with `.tar`) and the shell code.  
