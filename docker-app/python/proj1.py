import os
import glob
from collections import Counter

data_dir = '/data'
out_dir = '/output'


# out_file = f'{out_dir}/result.txt'


def process_files():
    curr_dir = os.getcwd()
    # os.chdir(curr_dir + dir)
    text_files = [file for file in glob.glob("*.txt")]
    #print(text_files)
    file_names = [os.path.abspath(f) for f in text_files]
    # print(file_names)
    file_info = {}
    words = 0
    # paths = [os.path.join(dir, fn) for fn in next(os.walk(dir))[2]]
    # print(paths)
    # f = open(out_file, "a+")
    for p in file_names:
        file = open(p, "r", encoding="utf-8-sig")
        c = Counter(file.read().split())
        word_count = sum(c.values())
        words += word_count
        file_info[os.path.basename(p)] = word_count

    largest = max(file_info.keys(), key=(lambda k: file_info[k]))
    # print(f"File name with maximum number of words: {largest}")
    # print(file_info)

    # change dir to /home/output and write to result.txt
    # change from /home/data/ to  /home
    os.chdir("../")
    # change to /home/output
    output_dir = os.getcwd() + out_dir
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    os.chdir(output_dir)
    with open("result.txt", "w+") as outfile:
        outfile.write("Here are your text files\n")
        for i, item in enumerate(text_files):
            outfile.write(f"{i + 1}. {item}\n")
        outfile.write(f"\nFile name with maximum number of words: {largest}\n")
        outfile.write(f"Sum of all words in your text data: {words}\n")

    with open("result.txt", "r") as fp:
        line = fp.readline()
        while line:
            print("{}".format(line.strip()))
            line = fp.readline()


process_files()

