package tests;

import bufmgr.*;
import global.SystemDefs;

import java.io.IOException;

public class Phase3Utils {
    public static void writeToDisk() {
        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
