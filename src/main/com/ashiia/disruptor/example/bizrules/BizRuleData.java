package com.ashiia.disruptor.example.bizrules;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.EntryFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

/**
 * User: mbharadwaj
 * Date: 7/25/11
 */
public class BizRuleData extends AbstractEntry implements Serializable {
    private long value;
    private boolean valid = false;
    private boolean saved = false;

    public void setValue(long value) {
        this.value = value;
    }

    public final static EntryFactory<BizRuleData> ENTRY_FACTORY = new EntryFactory<BizRuleData>() {
        public BizRuleData create() {
            return new BizRuleData();
        }
    };

    public BizRuleData validate(long mod) {
        try {
            valid = value % mod == 0;
        } catch (Exception e) {

        }
        return this;
    }
    public BizRuleData save() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);
            oos.writeObject(this);
            oos.close();
            byteArrayOutputStream.close();
            saved = true;
        } catch (Exception e) {

        }
        return this;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean isSaved() {
        return saved;
    }

}
