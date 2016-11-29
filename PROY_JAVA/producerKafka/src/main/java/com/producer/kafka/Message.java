package com.producer.kafka;

/**
 * Created by jsirpa on 16-11-16.
 */
public class Message {
    private String [] options;

    public Message(String [] args){
        options=new String[args.length];
        for (int i=0; i< args.length; i++)
        {
            this.options[i]=args[i];
        }
    }

    public String [] create(String [] vargs){
        String [] cad=new String[2];
        cad[1]="\0";
        cad[0]=options[0] + vargs[0];
        for (int i=1 ; i< options.length;i++)
        {
            cad[1]=cad[1] + options[i] + vargs[i];
        }
        return cad;
    }
}
