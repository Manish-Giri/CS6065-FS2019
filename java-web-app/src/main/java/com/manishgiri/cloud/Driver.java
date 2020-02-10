package com.manishgiri.cloud;

import org.slf4j.impl.SimpleLoggerFactory;
import spark.ModelAndView;
import spark.template.handlebars.HandlebarsTemplateEngine;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;

public class Driver {


    private static List<String> readFromFile(String filename) {
        File file = new File(filename);
        List<String> fileContents = new ArrayList<>();
        String line;
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            while((line = br.readLine())!= null) {
                fileContents.add(line);
            }
        }
        catch (IOException e) {
            System.out.println("Error: " + e);
        }
        return fileContents;
    }

    public static <br> void main(String[] args) {

        staticFileLocation("/public");

        get("/", (req, res) -> new ModelAndView(null, "launch.hbs"), new HandlebarsTemplateEngine());

        get("/sign-up", (req, res) -> new ModelAndView(null, "userform.hbs"), new HandlebarsTemplateEngine());

        // TODO: add GET request handler for /login

        get("/login", (req, res) -> new ModelAndView("null", "loginform.hbs"), new HandlebarsTemplateEngine());


        post("/verify", (req, res) -> {
            // get login info from form
            String username = req.queryParams("username");
            String password = req.queryParams("password");

            // read from file
            //File loginFile = new File("login.txt");
            //File detailsFile = new File("details.txt");
            String line;
            boolean isValid = false;

            List<String> loginFile = readFromFile("login.txt");
            System.out.println(loginFile);
            String u = loginFile.get(0);
            String p = loginFile.get(1);
            if(username.equals(u) && password.equals(p)) {
                isValid = true;
                List<String> detailsFile = readFromFile("details.txt");
                String f = detailsFile.get(0);
                String l = detailsFile.get(1);
                String e = detailsFile.get(2);
                Map<String, String> model = new HashMap<>();

                model.put("firstname", f);
                model.put("lastname", l);
                model.put("email", e);
                model.put("username", u);
                return new ModelAndView(model, "welcome.hbs");


            }
            else {
                return new ModelAndView(null, "invalidlogin.hbs");
            }
        }, new HandlebarsTemplateEngine());


        post("/register", (req, res) -> {
            Map<String, String> model = new HashMap<>();
            String u = req.queryParams("user");
            String p = req.queryParams("password");
            res.cookie("username", u);
            req.attribute("username", u);
            //res.cookie("password", p);
            model.put("username", u);

            // TODO: store username + password in text file
            File file = new File("login.txt");
            try(BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
                bw.write(u);
                bw.newLine();
                bw.write(p);
                //bw.newLine();
            }

            catch (IOException e) {
                System.out.println("Error: " + e);
            }



            return new ModelAndView(model, "details.hbs");
        }, new HandlebarsTemplateEngine());


        post("/welcome", (req, res) -> {
            Map<String, String> model = new HashMap<>();
            String f = req.queryParams("firstname");
            String l = req.queryParams("lastname");
            String e = req.queryParams("email");
            String u = req.cookie("username");
            //res.cookie("username", u);
            //res.cookie("password", p);
            res.cookie("firstname", f);
            res.cookie("lastname", l);
            res.cookie("email", e);

            File file = new File("details.txt");
            try(BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
                bw.write(f);
                bw.newLine();
                bw.write(l);
                bw.newLine();
                bw.write(e);

            }

            catch (IOException e1) {
                System.out.println("Error: " + e1);
            }


            model.put("firstname", f);
            model.put("lastname", l);
            model.put("email", e);
            model.put("username", u);
            return new ModelAndView(model, "welcome.hbs");
        }, new HandlebarsTemplateEngine());

    }
}
