import os
import glob
from collections import Counter

data_dir = '/data'
out_dir = '/output'
out_file = f'{out_dir}/result.txt'


def process_files(dir):
    curr_dir = os.getcwd()
    os.chdir(curr_dir + dir)
    text_files = [file for file in glob.glob("*.txt")]
    print(text_files)
    file_names = [os.path.abspath(f) for f in text_files]
    print(file_names)
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
    print(f"File name with maximum number of words: {largest}")
    print(file_info)


    # change dir to /home/output and write to result.txt
    os.chdir("../")
    output_dir = os.getcwd() + out_dir
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    os.chdir(output_dir)
    with open("result.txt", "w+") as outfile:
        outfile.write("Here are your text files\n")
        for i,item in enumerate(text_files):
            outfile.write(f"{i+1}. {item}\n")
        outfile.write(f"\nFile name with maximum number of words: {largest}\n")
        outfile.write(f"Sum of all words in your text data: {words}\n")



def run():
    #data = list_text_files(directory)
    #print(data)
    process_files(data_dir)


run()






# print(list_text_files(directory))
#process_files(directory)

# def list_files(dir):
#     os.chdir(dir)
#     print("Here are your text data.")
#     if not os.path.exists(out_dir):
#         os.mkdir(out_dir)
#     f = open(out_file, "w+")
#     i = 0
#     f.write("Here are your text data.\n")
#     for file in glob.glob("*.txt"):
#         f.write(f"{i+1}. {file}\n")
#         i += 1
#         print(file)
#     # add blank line
#     f.write("\n")
#     f.close()
#     # mylist = [f for f in glob.glob("*.txt")]

# def process_files(dir):
#     file_info = {}
#     words = 0
#     paths = [os.path.join(dir, fn) for fn in next(os.walk(dir))[2]]
#     print(paths)
#     f = open(out_file, "a+")
#     for p in paths:
#         file = open(p, "r", encoding="utf-8-sig")
#         c = Counter(file.read().split())
#         word_count = sum(c.values())
#         words += word_count
#         file_info[os.path.basename(p)] = word_count
#
#     largest = max(file_info.keys(), key=(lambda k: file_info[k]))
#
#     print(file_info)
#     print(f"File name with maximum number of words: {largest}")
#     f.write(f"File name with maximum number of words: {largest}\n")
#     f.write(f"Sum of all words in your text data: {words}\n")
#     f.close()