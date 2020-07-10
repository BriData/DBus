package com.creditease.dbus.heartbeat.distributed.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    public static final String NL = System.getProperty("line.separator");

    private static final Logger log = LoggerFactory.getLogger(org.apache.kafka.common.utils.Utils.class);

    public static <T> String join(T[] strs, String seperator) {
        return join(Arrays.asList(strs), seperator);
    }

    public static <T> String join(Collection<T> list, String seperator) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = list.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext())
                sb.append(seperator);
        }
        return sb.toString();
    }

    public static ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            return Utils.class.getClassLoader();
        else
            return cl;
    }

    public static <T extends Comparable<? super T>> List<T> sorted(Collection<T> collection) {
        List<T> res = new ArrayList<>(collection);
        Collections.sort(res);
        return Collections.unmodifiableList(res);
    }

    public static String utf8(byte[] bytes) {
        try {
            return new String(bytes, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    public static byte[] utf8(String string) {
        try {
            return string.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return (in.read() << 8 * 0)
                | (in.read() << 8 * 1)
                | (in.read() << 8 * 2)
                | (in.read() << 8 * 3);
    }

    public static byte[] toArrayLE(int val) {
        return new byte[] {
                (byte) (val >> 8 * 0),
                (byte) (val >> 8 * 1),
                (byte) (val >> 8 * 2),
                (byte) (val >> 8 * 3)
        };
    }


    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset++] << 8 * 0)
                | (buffer[offset++] << 8 * 1)
                | (buffer[offset++] << 8 * 2)
                | (buffer[offset]   << 8 * 3);
    }

    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value >>> 8 * 0);
        out.write(value >>> 8 * 1);
        out.write(value >>> 8 * 2);
        out.write(value >>> 8 * 3);
    }

    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset++] = (byte) (value >>> 8 * 0);
        buffer[offset++] = (byte) (value >>> 8 * 1);
        buffer[offset++] = (byte) (value >>> 8 * 2);
        buffer[offset]   = (byte) (value >>> 8 * 3);
    }


    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    public static long min(long first, long ... rest) {
        long min = first;
        for (int i = 0; i < rest.length; i++) {
            if (rest[i] < min)
                min = rest[i];
        }
        return min;
    }

    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.limit());
    }

    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    public static <T> T notNull(T t) {
        if (t == null)
            throw new NullPointerException();
        else
            return t;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
        }
    }

    public static <T> T newInstance(Class<T> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        } catch (NullPointerException e) {
            throw new RuntimeException("Requested class was null", e);
        }
    }

    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        return Utils.newInstance(Class.forName(klass, true, Utils.getClassLoader()).asSubclass(base));
    }

    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                ? "[" + host + "]:" + port // IPv6
                : host + ":" + port;
    }
}
