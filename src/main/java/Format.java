import java.io.*;

public class Format {

    public static byte[] msgToBytes(Object obj) throws IOException{
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);) {
            oos.writeObject(obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] serializedData = byteArrayOutputStream.toByteArray();
        return serializedData;
    }

    public static Command.BaseCommand bytesToEntry(byte[] bytes) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Command.BaseCommand entry = (Command.BaseCommand) objectInputStream.readObject();
            return entry;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new ClassCastException("base message class not found");
        }
    }
}
