import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class CSV_Reader
{

    public static void main(String[] args) throws IOException, InterruptedException {
        String hostName = "localhost";
        int portNumber = 9999;

        Socket echoSocket = new Socket(hostName, portNumber);
        BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")+"/alldates.csv"));
        String line;
        PrintWriter out = new PrintWriter(echoSocket.getOutputStream(), true);

        while ((line = br.readLine()) != null){
            out.println(line);
            Thread.sleep(5000);
        }

        echoSocket.close();
        br.close();
        out.close();
    }


}
