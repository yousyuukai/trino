/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client.auth.kerberos;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.sun.security.auth.module.Krb5LoginModule;
import io.trino.client.ClientException;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.File;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.getBoolean;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

public class LoginBasedUnconstrainedContextProvider
        extends AbstractUnconstrainedContextProvider
{
    private final Optional<String> principal;
    private final Optional<File> keytab;
    private final Optional<File> credentialCache;

    @GuardedBy("this")
    private LoginContext loginContext;

    public LoginBasedUnconstrainedContextProvider(
            Optional<String> principal,
            Optional<File> kerberosConfig,
            Optional<File> keytab,
            Optional<File> credentialCache)
    {
        this.principal = requireNonNull(principal, "principal is null");
        this.keytab = requireNonNull(keytab, "keytab is null");
        this.credentialCache = requireNonNull(credentialCache, "credentialCache is null");

        kerberosConfig.ifPresent(file -> {
            String newValue = file.getAbsolutePath();
            String currentValue = System.getProperty("java.security.krb5.conf");
            checkState(
                    currentValue == null || Objects.equals(currentValue, newValue),
                    "Refusing to set system property 'java.security.krb5.conf' to '%s', it is already set to '%s'",
                    newValue,
                    currentValue);
            checkState(
                    file.exists() && !file.isDirectory(),
                    "Kerberos config file '%s' does not exist or is a directory",
                    newValue);
            checkState(file.canRead(), "Kerberos config file '%s' is not readable", newValue);
            System.setProperty("java.security.krb5.conf", newValue);
        });
    }

    @Override
    public synchronized Subject getSubject()
    {
        return loginContext.getSubject();
    }

    @Override
    public synchronized void refresh()
            throws GSSException
    {
        // TODO: do we need to call logout() on the LoginContext?
        try {
            loginContext = new LoginContext("", null, null, new Configuration()
            {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name)
                {
                    ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
                    options.put("refreshKrb5Config", "true");
                    options.put("doNotPrompt", "true");
                    options.put("useKeyTab", "true");

                    if (getBoolean("trino.client.debugKerberos")) {
                        options.put("debug", "true");
                    }

                    keytab.ifPresent(file -> options.put("keyTab", file.getAbsolutePath()));

                    credentialCache.ifPresent(file -> {
                        options.put("ticketCache", file.getAbsolutePath());
                        options.put("renewTGT", "true");
                    });

                    if (!keytab.isPresent() || credentialCache.isPresent()) {
                        options.put("useTicketCache", "true");
                    }

                    principal.ifPresent(value -> options.put("principal", value));

                    return new AppConfigurationEntry[] {
                            new AppConfigurationEntry(Krb5LoginModule.class.getName(), REQUIRED, options.buildOrThrow())
                    };
                }
            });

            loginContext.login();
        }
        catch (LoginException e) {
            throw new ClientException(format("Kerberos login error for [%s]: %s", principal.orElse("not defined"), e.getMessage()), e);
        }
    }
}
