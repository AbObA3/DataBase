import Database.DataBaseImpl;

public class Run {

    public static void main(String[] args) {
        DataBaseImpl dataBase = new DataBaseImpl();
        dataBase.createTable();
        dataBase.write();
    }
}
