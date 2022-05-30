package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.example.data.RequestData;
import org.example.data.ResponseData;

import java.sql.*;

/**
 * Insert data using lambda function and aws rds mysql!
 * mvn clean package shade:shade
 * {
 *     "rollNumber" : "1",
 * }
 */
public class StudentDeleteApp implements RequestHandler<RequestData, ResponseData>
{
    @Override
    public ResponseData handleRequest(RequestData requestData, Context context) {
        ResponseData responseData = new ResponseData();
        try {
            if (getValidate(requestData, responseData)){
                deleteData(requestData,responseData);
            }
        } catch (SQLException sqlException) {
            responseData.setMessageId("999");
            responseData.setMessage("Unable to Delete "+sqlException);
        }
        return responseData;
    }

    private void deleteData(RequestData requestData, ResponseData responseData) throws SQLException{
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String query = getQuery(requestData);
        int responseCode = statement.executeUpdate(query);
        if (1 == responseCode){
            responseData.setMessageId(String.valueOf(responseCode));
            responseData.setMessage("Successful delete data");
        }
    }

    private Boolean getValidate(RequestData requestData, ResponseData responseData) throws SQLException{
        Boolean status = false;
        String query = "SELECT * FROM `student` WHERE `student`.`student_roll` = ";
        if (requestData != null){
            query = query.concat(requestData.getRollNumber());
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            if (rs.next()){
                status = true;
            } else {
                status = false;
                responseData.setMessageId(String.valueOf(rs));
                responseData.setMessage("Data not Found");
            }
        } else {
            status = false;
            responseData.setMessageId("999");
            responseData.setMessage("Invalid request");
        }
        return status;
    }

    private String getQuery(RequestData requestData){
        String deleteQuery = "DELETE FROM `student` WHERE `student`.`student_roll` = ";
        if (requestData != null){
            deleteQuery = deleteQuery.concat("'"+requestData.getRollNumber()+"'");
        }
        return deleteQuery;
    }

    private Connection getConnection() throws SQLException{
        String url = "jdbc:mysql://localhost:3306/gopidatbase";
        String username = "root";
        String password = "";
        Connection con = DriverManager.getConnection(url,username,password);
        return con;
    }
}
