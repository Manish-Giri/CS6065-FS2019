package com.manishgiri.cloud;

import com.manishgiri.cloud.dao.UserDAOImpl;
import com.manishgiri.cloud.fileupload.FileHandler;
import com.manishgiri.cloud.model.User;
import spark.ModelAndView;
import spark.Session;
import spark.template.handlebars.HandlebarsTemplateEngine;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;
import static spark.Spark.post;

public class Application {



    public static void main(String[] args) {

        UserDAOImpl dao = new UserDAOImpl();

        // staticFileLocation("/public");

        get("/", (req, res) -> new ModelAndView(null, "launch.hbs"), new HandlebarsTemplateEngine());

        get("/sign-up", (req, res) -> new ModelAndView(null, "userform.hbs"), new HandlebarsTemplateEngine());

        // TODO: add GET request handler for /login

        get("/login", (req, res) -> new ModelAndView("null", "loginform.hbs"), new HandlebarsTemplateEngine());

        // Check if user exists in DB (correct login)
        post("/verify", (req, res) -> {
            // get login info from form
            String username = req.queryParams("username").trim();
            String password = req.queryParams("password").trim();

            // set username in session
            req.session().attribute("username", username);

            User user = dao.findUser(username, password);

            if(user != null) {
                String f = user.getFirstName();
                String l = user.getLastName();
                String e = user.getEmail();

                return getModelAndView(f, l, e, username);
            }

            else {
                return new ModelAndView(null, "invalidlogin.hbs");
            }
        }, new HandlebarsTemplateEngine());



        post("/register", (req, res) -> {
            String u = req.queryParams("user").trim();
            String p = req.queryParams("password").trim();
            boolean userAdded = dao.addUser(u, p);
            if(userAdded) {
                res.cookie("username", u);
                req.attribute("username", u);
                // create user session
                Session session = req.session(true);
                session.attribute("username", u);
                return getModelAndView(u);
            }
            else {
                return new ModelAndView(null, "invalidlogin.hbs");
            }
        }, new HandlebarsTemplateEngine());


        post("/welcome", (req, res) -> {

            String f = req.queryParams("firstname").trim();
            String l = req.queryParams("lastname").trim();
            String e = req.queryParams("email").trim();
            String u = req.cookie("username");

            boolean userAdded = dao.addUserDetails(f, l, e, u);
            if(userAdded) {
                res.cookie("firstname", f);
                res.cookie("lastname", l);
                res.cookie("email", e);

                return getModelAndView(f, l, e, u);
            }

            else {
                return new ModelAndView(null, "invalidlogin.hbs");
            }

        }, new HandlebarsTemplateEngine());



        // FILE HANDLING

        // GET /file for form to upload file
        get("/file", (req, res) -> {
            Map<String, String> model = new HashMap<>();
            String username = req.session().attribute("username");
            System.out.println("Username from session: " + username);
            model.put("username", username);
            return new ModelAndView(model, "uploadfile.hbs");

        }, new HandlebarsTemplateEngine());

        // POST /upload for handing form
        post("/upload", (req, res) -> {
            /*String STORAGE = "storage";
            File storageDir = new File(STORAGE);
            if (!storageDir.isDirectory()) storageDir.mkdir();*/

            // empty storage folder
            //FileHandler.deleteStorage("storage");


            // get username from session
            String username = req.session().attribute("username");
            System.out.println("Username from session: " + username);

            // get filename
            //String filename = req.queryParams("title");
            //String filename = req.queryParams("file");


            // set filename in session


            // get wordcount in file
            //int wordCount = FileHandler.fileWordCount(filename);


            String rep = FileHandler.uploadFile(req);
            //System.out.println(rep);
            //int wordCount = FileHandler.getWordCount();
            int wordCount = FileHandler.fileWordCount();

            //int wordCount = FileHandler.fileWordCountVerOne();

            String filename = FileHandler.uploadedFile;
            System.out.println("filename: " + filename);
            req.session().attribute("fileName", filename);
            Map<String, String> model = new HashMap<>() ;
            //model.put("result", rep);
            model.put("username", username);
            model.put("filename", filename);
            model.put("wordcount", String.valueOf(wordCount));
            //res.body(FileHandler.uploadFile(req));
            return new ModelAndView(model, "uploadfileresult.hbs");
        }, new HandlebarsTemplateEngine());


        get("/count", (req, res) -> {
            int count = FileHandler.countFiles();
            System.out.println("No. of files:  " + count);
            Map<String, String> model = new HashMap<>() ;
            model.put("result", String.valueOf(count));
            //res.body(FileHandler.uploadFile(req));
            return new ModelAndView(model, "uploadfileresult.hbs");
        }, new HandlebarsTemplateEngine());


/*        get("/download/:file", (req, res) -> {
            String fileContents = FileHandler.downloadFile(req.params(":file"));
            Map<String, String> model = new HashMap<>() ;
            model.put("result", String.valueOf(fileContents));
            //res.body(FileHandler.uploadFile(req));
            return new ModelAndView(model, "uploadfileresult.hbs");
        }, new HandlebarsTemplateEngine());*/


        get("/downloadfile", (req, res) -> {
            Map<String, String> model = new HashMap<>();
            String username = req.session().attribute("username");
            System.out.println("Username from session: " + username);
            model.put("username", username);
            return new ModelAndView(model, "downloadfile.hbs");
        }, new HandlebarsTemplateEngine());

        get("/download", (req, res) -> {
            String fileName = req.queryParams("fname");
            // FileHandler.downloadFileVerTwo(res, req.params(":file"));
            HttpServletResponse response = FileHandler.downloadFileVerTwo(res, fileName);
            Map<String, String> model = new HashMap<>();
            String username = req.session().attribute("username");
            System.out.println("Username from session: " + username);
            model.put("username", username);
            // return new ModelAndView(model, "uploadfile.hbs");
            if(response == null) {
                return new ModelAndView(model, "filenotfound.hbs");
            }
            return null;
        }, new HandlebarsTemplateEngine());


    }

    private static ModelAndView getModelAndView(String u) {
        Map<String, String> model = new HashMap<>();
        model.put("username", u);
        return new ModelAndView(model, "details.hbs");
    }
    private static ModelAndView getModelAndView(String f, String l, String e, String u) {
        Map<String, String> model = new HashMap<>();

        model.put("firstname", f);
        model.put("lastname", l);
        model.put("email", e);
        model.put("username", u);
        return new ModelAndView(model, "welcome.hbs");
    }
}
