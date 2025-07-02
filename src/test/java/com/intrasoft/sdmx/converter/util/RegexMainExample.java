package com.intrasoft.sdmx.converter.util;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMainExample {
    public static void main(String[] args){    
        Scanner sc=new Scanner(System.in);  
        while (true) {    
            System.out.println("Enter regex pattern:");
            String patternString = sc.nextLine();
            Pattern pattern = Pattern.compile(patternString);    
            System.out.println("Enter text:");  
            String matcherString = sc.nextLine();
            Matcher matcher = pattern.matcher(matcherString);    
            boolean found = false;    
            while (matcher.find()) {    
                System.out.println("I found the text "+matcher.group()+" starting at index "+    
                 matcher.start()+" and ending at index "+matcher.end());    
                found = true;    
            }    
            if(!found){    
                System.out.println("No match found.");    
            }    
        }    
    }    
}
