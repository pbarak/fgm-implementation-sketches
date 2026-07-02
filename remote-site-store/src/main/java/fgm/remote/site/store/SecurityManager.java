package fgm.remote.site.store;

import java.security.Permission;

public class SecurityManager extends java.lang.SecurityManager {
    //empty permission
    @Override
    public void checkPermission(Permission perm) {
        return;
    }
}
