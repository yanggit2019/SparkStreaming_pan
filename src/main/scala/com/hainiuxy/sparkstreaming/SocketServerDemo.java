package com.hainiuxy.sparkstreaming;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServerDemo {
    public static void main(String[] args) throws IOException
    {
        ServerSocket serverSocket = new ServerSocket(6666);//这个端口自己随意,建议1024以上未使用的端口.
        int n = 10000;
        while(true)
        {
            Socket socket = serverSocket.accept();//一直等待来自客户端的请求.

            PrintStream printStream = new PrintStream(socket.getOutputStream());//创建输出流
            for(int i = 1; i<= n; i++){
                printStream.println("aa");
            }
            n += 10000;
            printStream.close();
            socket.close();
        }
    }
}
