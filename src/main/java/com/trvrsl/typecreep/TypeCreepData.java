package com.trvrsl.typecreep;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by lyonwj on 4/26/14.
 */
public class TypeCreepData {
    public String getUser() {
        return user;
    }

    public ArrayList<Map> getData() {
        return data;
    }

    private String user;
    private ArrayList<Map> data;

    public void setData(ArrayList<Map> data) {
        this.data = data;
    }

    public void setUser(String user) {
        this.user = user;
    }



    public TypeCreepData(){

    }

    public TypeCreepData(String user, ArrayList<Map> data){
        this.user = user;
        this.data = data;
    }


}
