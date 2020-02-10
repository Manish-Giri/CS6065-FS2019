package com.manishgiri.cloud.fileupload;


import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;

import static spark.Spark.*;

public class FileHandler {

    private static String STORAGE = "storage";
    private static int wordCount;
    public static String uploadedFile = "";
    public static boolean isFileInvalid = false;

    public static int getWordCount() {
        return wordCount;
    }

    public static void main(String[] args) {
        File storageDir = new File(STORAGE);
        if (!storageDir.isDirectory()) storageDir.mkdir();

        post("/upload", (req, res) -> uploadFile(req));
        get("/download/:file", (req, res) -> downloadFile(req.params(":file")));
        get("/count", (req, res) -> countFiles());
        delete("/delete/:file", (req, res) -> deleteFile(req.params(":file")));

    }

    public static String uploadFile(Request request) {
        // TO allow for multipart file uploads
        String STORAGE = "storage";
        File storageDir = new File(STORAGE);
        if (!storageDir.isDirectory()) storageDir.mkdir();
        //request.contentType("UTF-8");
        request.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement(""));

        try {
            // "file" is the key of the form data with the file itself being the value
            Part filePart = request.raw().getPart("file");
            // The name of the file user uploaded
            String uploadedFileName = filePart.getSubmittedFileName();
            //wordCount = fileWordCount(uploadedFileName);
            //wordCount = fileWordCount(uploadedFileName);
            //System.out.println("words: " + wordCount);
            uploadedFile = uploadedFileName;

            InputStream stream = filePart.getInputStream();

            // Write stream to file under storage folder
            Files.copy(stream, Paths.get(STORAGE).resolve(uploadedFileName),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | ServletException e) {
            return "Exception occurred while uploading file" + e.getMessage();
        }

        return "File successfully uploaded";
    }

    public static String downloadFile(String fileName) {
        Path filePath = Paths.get(STORAGE).resolve(fileName);
        File file = filePath.toFile();
        if (file.exists()) {
            try {
                // Read from file and join all the lines into a string
                return Files.readAllLines(filePath).stream().collect(Collectors.joining());
            } catch (IOException e) {
                return "Exception occurred while reading file" + e.getMessage();
            }

        }
        return "File doesn't exist. Cannot download";
    }

  /*  public static void downloadFileVerTwo(String fileName) {
        try {
            URL url = new URL(Paths.get(STORAGE).resolve(fileName).toString());
            FileUtils.copyURLToFile(url, new File("hello.txt")) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        //FileUtils.copyURLToFile(URL.fromURI(Paths.get(STORAGE).resolve(fileName).toUri()), new File("hello.txt")) ;
    }*/

    public static HttpServletResponse downloadFileVerTwo(Response res, String fileName) {
//        res.header("Content-Type", "application/download");
//        res.header("Content-Disposition", "attachment; filename="+ fileName);

        HttpServletResponse raw = res.raw();

        try {
            byte[] bytes = Files.readAllBytes(Paths.get(STORAGE).resolve(fileName));
            res.header("Content-Type", "application/download");
            res.header("Content-Disposition", "attachment; filename="+ fileName);
            raw.getOutputStream().write(bytes);
            raw.getOutputStream().flush();
            raw.getOutputStream().close();
        }
        catch (NoSuchFileException | NullPointerException e) {
            return null;
        }
          catch (IOException e) {
                isFileInvalid = true;
                e.printStackTrace();
          }


        return res.raw();
    }

    public static int countFiles() {
        // Count the number of files in the storage folder
        return Objects.requireNonNull(new File(STORAGE).listFiles()).length;
    }


    public static int fileWordCountVerOne() {
        Path filePath = Paths.get(STORAGE).resolve(uploadedFile);
        //File file = filePath.toFile();



        int wordCount = -1;

        try {
            long uniqueWords = Files
                    .lines(filePath, Charset.defaultCharset())
                    .flatMap(line -> Arrays.stream(line.split(" .")))
                    .count();
            wordCount = (int) uniqueWords;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return wordCount;
    }

    public static int fileWordCount() {
        int count = 0;
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(STORAGE).resolve(uploadedFile));
            //System.out.println("bytes array: " + Arrays.toString(bytes));
            String s = new String(bytes);
            String original = new String (s.getBytes (StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
            System.out.println("String in file: " + original);
            String [] data = original.split(" ");
            for (int i=0; i<data.length; i++) {
                count++;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return count;
    }


    public static String deleteFile(String fileName) {

        File file = Paths.get(STORAGE).resolve(fileName).toFile();
        if (file.exists()) {
            boolean delete = file.delete();

            return delete ? "File deleted" : "File could not be deleted";
        } else {
            return "File " + fileName + " doesn't exist";
        }
    }


    public static boolean deleteStorage(String folderName) {
        Path directory = Paths.get(folderName);
        boolean deleted = true;
        try {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
            deleted = false;
        }
        return deleted;
    }

}
