/*
 * Hazelcast Space for JPOS
 * Copyright (C) 2012 Eric Reuthe/ Wired-Mind Labs, LLC
 *
 * Code based on Alejandro Revilla's
 * TSpace implementation
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jpos.space;

import java.io.FileNotFoundException;
import java.io.NotSerializableException;
import java.io.PrintStream;
import java.util.Set;
import java.util.LinkedList;
import java.util.*;
import java.io.Serializable;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.nio.Serializer.DataSerializer;
import com.hazelcast.core.*;

import org.jpos.util.Loggeable;
import org.jpos.core.Configuration;
import java.util.concurrent.*;


/**
 * HzlSpace implementation for JPOS
 *
 * @author Eric Reuthe
 * @version $Revision$ $Date$
 */

public class HzlSpace<K, V> implements LocalSpace<K, V>, Loggeable {
    private volatile HazelcastInstance instance;
    private volatile HazelcastClient client;
    private DataSerializer serializer = new DataSerializer();
    private String hzlConfigFile;
    private String spaceName;
    private String clusterName;
    private String clusterPassword;
    private boolean useShuffle;
    private String clusterIPs;
    protected Configuration cfg;
    protected Config hzlConfig;
    protected TSpace<K,V> liveObjectSpace = new TSpace<K,V>();
    protected IMap<Object, LinkedList<Object>> entries;
    protected HzlSpace sl;    // space listeners
    protected IList<Set> expirables;
    protected ScheduledExecutorService cleanupScheduler;
    public static final long GCDELAY = 60 * 1000;
    private static final long GCLONG = GCDELAY * 5;
    private long lastLongGC = System.currentTimeMillis();
    public boolean clientOnly = false;


    public HzlSpace(String spaceName) {
        createSpace(spaceName, "");
    }

    public HzlSpace(String spaceName, String configFile) {
        createSpace(spaceName, configFile);
    }

    public HzlSpace(Configuration cfg) {
        this.cfg = cfg;
        this.spaceName = cfg.get("space-name", "hzl:DefaultSpace");
        this.clientOnly = cfg.getBoolean("clientOnly", false);
        if (this.clientOnly) {
            createClient(this.cfg);
        }  else {
            createSpace(spaceName, "");
        }
    }

    public HzlSpace() {
        createSpace("hzl:DefaultSpace", "");
    }

    public void setHazelcastInstance(HazelcastInstance instance) {
        this.instance = instance;
    }

    private void createClient(Configuration cfg) {
        this.clusterName = this.cfg.get("clusterName");
        this.clusterPassword = this.cfg.get("clusterPassword");
        this.useShuffle = this.cfg.getBoolean("useShuffle", false);
        this.clusterIPs = this.cfg.get("clusterIPs");
        GroupConfig groupConf = new GroupConfig();
        groupConf.setName(this.clusterName);
        groupConf.setPassword(this.clusterPassword);

        ClientConfig clientConf = new ClientConfig();
        clientConf.setShuffle(this.useShuffle);
        clientConf.addAddress(this.clusterIPs);
        clientConf.setGroupConfig(groupConf);
        clientConf.setReconnectionAttemptLimit(5);

        this.client = HazelcastClient.newHazelcastClient(clientConf);
        entries = this.client.getMap(spaceName);
        expirables = this.client.getList(spaceName + "-expirables");
    }

    private void createSpace(String spaceName, String configFile) {

        if (configFile != null && !configFile.isEmpty()) {
            this.hzlConfigFile = configFile;
        } else {
            if (this.cfg != null) {
                this.hzlConfigFile = cfg.get("hzlConfigFile", "");
            }
        }

        if (this.hzlConfigFile != null && !this.hzlConfigFile.isEmpty()) {
            try {
                this.hzlConfig = new XmlConfigBuilder(this.hzlConfigFile).build();
            } catch (FileNotFoundException e) {
                throw new SpaceError(e);
            }
        }

        try {
            if (this.cfg != null && this.hzlConfig != null) {
                this.instance = Hazelcast.init(this.hzlConfig);
            } else {
                this.instance = Hazelcast.getDefaultInstance();
            }
            entries = this.instance.getMap(spaceName);
            expirables = this.instance.getList(spaceName + "-expirables");
        } catch (Exception e) {
            e.printStackTrace();
        }
        entries = this.instance.getMap(spaceName);
        expirables = this.instance.getList(spaceName + "-expirables");

        expirables.add(new HashSet<K>());
        expirables.add(new HashSet<K>());

        cleanupScheduler = Executors.newScheduledThreadPool(1);
    }

    public boolean isLiveObject(Object obj) {
        return !this.serializer.isSuitable(obj);

    }

    public void out(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);
        synchronized (this) {
            if (isLiveObject(value)) {
                liveObjectSpace.out(key, value);
                value = (V)new LiveObject(key, value);
            }
            LinkedList<Object> l = entries.get(key);
            if (l == null) l = new LinkedList<Object>();
            l.add(value);
            entries.put(key, l);
            this.notifyAll();
            if (sl != null)
                notifyListeners(key, value);
        }
    }

    public void out(K key, V value, long timeout) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);
        Object v = value;
        if (timeout > 0) {
            v = new Expirable(value, System.currentTimeMillis() + timeout);
        }
        synchronized (this) {
            if (isLiveObject(value)) {
                liveObjectSpace.out(key, value, timeout);
                value = (V)new LiveObject(key, value);
            }
            LinkedList<Object> l = entries.get(key);
            if (l == null) l = new LinkedList<Object>();
            l.add(v);
            entries.put(key, l);
            this.notifyAll();
            if (sl != null)
                notifyListeners(key, value);
            if (timeout > 0) {
                registerExpirable(key, timeout);
            }
        }
    }

    public synchronized V rdp(Object key) {
        Object v = null;
        if (key instanceof Template)
            v = getObject((Template) key, false);
        else
            v = getHead(key, false);

        if (v instanceof LiveObject)
            v = liveObjectSpace.rdp(key);

        return (V)v;
    }

    public synchronized V inp(Object key) {
        Object v = null;
        if (key instanceof Template)
            v = getObject((Template) key, true);
        else
            v = getHead(key, true);

        if (v instanceof LiveObject)
            v = liveObjectSpace.inp(key);

        return (V)v;
    }

    public synchronized V in(Object key) {
        Object obj;
        while ((obj = inp(key)) == null) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
        return (V) obj;
    }

    public synchronized V in(Object key, long timeout) {
        Object obj;
        long now = System.currentTimeMillis();
        long end = now + timeout;
        while ((obj = inp(key)) == null &&
                ((now = System.currentTimeMillis()) < end)) {
            try {
                this.wait(end - now);
            } catch (InterruptedException e) {
            }
        }
        return (V) obj;
    }

    public synchronized V rd(Object key) {
        Object obj;
        while ((obj = rdp(key)) == null) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
        return (V) obj;
    }

    public synchronized V rd(Object key, long timeout) {
        Object obj;
        long now = System.currentTimeMillis();
        long end = now + timeout;
        while ((obj = rdp(key)) == null &&
                ((now = System.currentTimeMillis()) < end)) {
            try {
                this.wait(end - now);
            } catch (InterruptedException e) {
            }
        }
        return (V) obj;
    }

    public synchronized int size(Object key) {
        int size = 0;
        LinkedList<Object> l = entries.get(key);
        if (l != null)
            size = l.size();
        return size;
    }

    public synchronized void addListener(Object key, SpaceListener listener) {
        getSL().out(key, listener);
    }

    public synchronized void addListener
            (Object key, SpaceListener listener, long timeout) {
        getSL().out(key, listener, timeout);
    }

    public synchronized void removeListener
            (Object key, SpaceListener listener) {
        if (sl != null) {
            sl.inp(new ObjectTemplate(key, listener));
        }
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public Set getKeySet() {
        return entries.keySet();
    }

    public String getKeysAsString() {
        StringBuffer sb = new StringBuffer();
        Object[] keys;

        synchronized (this) {
            keys = entries.keySet().toArray();
        }

        for (int i = 0; i < keys.length; i++) {
            if (i > 0)
                sb.append(' ');
            sb.append(keys[i]);
        }
        return sb.toString();
    }

    public void dump(PrintStream p, String indent) {
        Object[] keys;
        synchronized (this) {
            keys = entries.keySet().toArray();
        }

        for (int i = 0; i < keys.length; i++) {
            p.printf("%s<key count='%d'>%s</key>\n", indent, size(keys[i]), keys[i]);
        }
        p.println(indent + "<keycount>" + (keys.length) + "</keycount>");
        int exp0, exp1;
        synchronized (this) {
            exp0 = expirables.get(0).size();
            exp1 = expirables.get(1).size();
        }
        p.println(String.format("%s<gcinfo>%d,%d</gcinfo>\n", indent, exp0, exp1));
    }

    public void notifyListeners(Object key, Object value) {
        Object[] listeners = null;

        synchronized (this) {
            if (sl == null)
                return;
            LinkedList<Object> l = (LinkedList<Object>) sl.entries.get(key);
            if (l != null)
                listeners = l.toArray();
        }
        if (listeners != null) {
            for (int i = 0; i < listeners.length; i++) {
                Object o = listeners[i];
                if (o instanceof Expirable)
                    o = ((Expirable) o).getValue();
                if (o instanceof SpaceListener)
                    ((SpaceListener) o).notify(key, value);
            }
        }
    }

    public void push(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);
        synchronized (this) {
            if (isLiveObject(value)) {
                liveObjectSpace.push(key, value);
                value = (V)new LiveObject(key, value);
            }
            LinkedList<Object> l = entries.get(key);
            if (l == null) l = new LinkedList<Object>();
            l.add(value);
            entries.put(0, l);
            this.notifyAll();
            if (sl != null)
                notifyListeners(key, value);
        }
    }

    public void push(K key, V value, long timeout) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);
        Object v = value;
        if (timeout > 0) {
            v = new Expirable(value, System.currentTimeMillis() + timeout);
        }
        synchronized (this) {
            if (isLiveObject(value)) {
                liveObjectSpace.push(key, value, timeout);
                value = (V)new LiveObject(key, value);
            }
            LinkedList<Object> l = entries.get(key);
            if (l == null) l = new LinkedList<Object>();
            l.add(v);
            entries.put(key, l);
            this.notifyAll();
            if (sl != null)
                notifyListeners(key, value);
            if (timeout > 0) {
                registerExpirable(key, timeout);
            }
        }
    }

    public synchronized void put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);

        if (isLiveObject(value)) {
            liveObjectSpace.put(key, value);
            value = (V)new LiveObject(key, value);
        }
        LinkedList<Object> l = new LinkedList<Object>();
        l.add(value);
        entries.put((Object)key, l);
        this.notifyAll();
        if (sl != null)
            notifyListeners(key, value);
    }

    public void put(K key, V value, long timeout) {
        if (key == null || value == null)
            throw new NullPointerException("key=" + key + ", value=" + value);
        Object v = value;
        if (timeout > 0) {
            v = new Expirable(value, System.currentTimeMillis() + timeout);
        }
        synchronized (this) {
            if (isLiveObject(value)) {
                liveObjectSpace.put(key, value, timeout);
                value = (V)new LiveObject(key, value);
            }
            LinkedList<Object> l = new LinkedList<Object>();
            l.add(v);
            entries.put(key, l);
            this.notifyAll();
            if (sl != null)
                notifyListeners(key, value);
            if (timeout > 0) {
                registerExpirable(key, timeout);
            }
        }
    }

    public boolean existAny(K[] keys) {
        for (int i = 0; i < keys.length; i++) {
            if (rdp(keys[i]) != null)
                return true;
        }
        return false;
    }

    public boolean existAny(K[] keys, long timeout) {
        long now = System.currentTimeMillis();
        long end = now + timeout;
        while (((now = System.currentTimeMillis()) < end)) {
            if (existAny(keys))
                return true;
            synchronized (this) {
                try {
                    wait(end - now);
                } catch (InterruptedException e) {
                }
            }
        }
        return false;
    }

    public Map getEntries() {
        return entries;
    }

    public void setEntries(IMap entries) {
        this.entries = entries;
    }

    protected static class LiveObject implements Serializable{
        Object localKey;
        Object distributedKey;
        Object distributedValue;
        Object liveValue;

        public LiveObject(Object key, Object value) {
            super();
            this.localKey = key;
            this.distributedKey = key;
            this.liveValue = value;
            this.distributedValue = this.localKey;
        }
    }

    private Object getHead(Object key, boolean remove) {
        Object obj = null;
        LinkedList<Object> l = entries.get(key);
        boolean wasExpirable = false;
        while (obj == null && l != null && l.size() > 0) {
            obj = l.get(0);
            if (obj == null) {
                l.remove(0);
                if (l.size() == 0) {
                    entries.remove(key);
                }
            }
        }
        if (obj != null && remove) {
            l.remove(0);
            if (l.size() == 0) {
                entries.remove(key);
                if (wasExpirable)
                    unregisterExpirable(key);
            }
        }
        return obj;
    }

    private Object getObject(Template tmpl, boolean remove) {
        Object obj = null;
        LinkedList<Object> l = entries.get(tmpl.getKey());
        if (l == null)
            return obj;

        Iterator iter = l.iterator();
        while (iter.hasNext()) {
            obj = iter.next();
            if (tmpl.equals(obj)) {
                if (remove)
                    iter.remove();
                break;
            } else
                obj = null;
        }
        return obj;
    }

    protected class ExpirationService implements Serializable, Runnable {

        public ExpirationService() {
            cleanupScheduler.scheduleAtFixedRate(this, GCDELAY, GCDELAY, TimeUnit.MILLISECONDS);
        }

        public void run() {
            try {
                gc();
            } catch (Exception e) {
                e.printStackTrace(); // this should never happen
            }
        }

        public void gc() {
            gc(0);
            if (System.currentTimeMillis() - lastLongGC > GCLONG) {
                gc(1);
                lastLongGC = System.currentTimeMillis();
            }
        }

        private void gc(int generation) {
            Set<K> exps = expirables.get(generation);
            synchronized (this) {
                expirables.set(generation, new HashSet<K>());
            }

            for (K k : exps) {
                if (rdp(k) != null) {
                    unregisterExpirable(k);
                    synchronized (this) {
                        expirables.get(generation).add(k);
                    }
                } else {
                    entries.remove(k);
                }
                Thread.yield();
            }
            if (sl != null) {
                synchronized (this) {
                    if (sl != null && sl.isEmpty())
                        sl = null;
                }
            }
        }
    }

    private HzlSpace getSL() {
        synchronized (this) {
            if (sl == null)
                sl = new HzlSpace();
        }
        return sl;
    }

    private void registerExpirable(K k, long t) {
        expirables.get(t > GCLONG ? 1 : 0).add(k);
    }

    private void unregisterExpirable(Object k) {
        for (Set<K> s : expirables)
            s.remove(k);
    }

    static class Expirable implements Comparable, Serializable {
        Object value;
        long expires;

        public Expirable(Object value, long expires) {
            super();
            this.value = value;
            this.expires = expires;
        }

        public boolean isExpired() {
            return expires < System.currentTimeMillis();
        }

        public String toString() {
            return getClass().getName()
                    + "@" + Integer.toHexString(hashCode())
                    + ",value=" + value.toString()
                    + ",expired=" + isExpired();
        }

        public Object getValue() {
            return isExpired() ? null : value;
        }

        public int compareTo(Object obj) {
            Expirable other = (Expirable) obj;
            long otherExpires = other.expires;
            if (otherExpires == expires)
                return 0;
            else if (expires < otherExpires)
                return -1;
            else
                return 1;
        }
    }
}