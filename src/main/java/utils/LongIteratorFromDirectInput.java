package utils;

import io.DirectReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LongIteratorFromDirectInput implements Iterator<Long> {

    private DirectReader reader;

    public LongIteratorFromDirectInput(String filePath) throws IOException {
        this.reader = new DirectReader(filePath);
    }

    @Override
    public boolean hasNext() {
        try {
            return reader.hasNext();
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Long next() {
        try {
            if(hasNext()) return reader.readLong();
            else{
                reader.close();
                throw new NoSuchElementException();
            }
        } catch (IOException e) {
            try {
                reader.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            throw new NoSuchElementException();
        }
    }
}
