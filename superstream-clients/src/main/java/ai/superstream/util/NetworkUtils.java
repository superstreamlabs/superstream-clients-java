package ai.superstream.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Utility class for network-related operations.
 */
public class NetworkUtils {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(NetworkUtils.class);
    private static String cachedIpAddress = null;
    private static String cachedHostname = null;

    /**
     * Get the local IP address.
     *
     * @return The local IP address, or "unknown" if it can't be determined
     */
    public static String getLocalIpAddress() {
        if (cachedIpAddress != null) {
            return cachedIpAddress;
        }

        try {
            // Try to get the primary network interface's IP address
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces == null) {
                logger.warn("No network interfaces found");
                return "";
            }
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.getHostAddress().contains(".")) { // Prefer IPv4
                        cachedIpAddress = address.getHostAddress();
                        return cachedIpAddress;
                    }
                }
            }

            // Fall back to the local host address
            InetAddress localHost = InetAddress.getLocalHost();
            cachedIpAddress = localHost.getHostAddress();
            return cachedIpAddress;
        } catch (SocketException | UnknownHostException e) {
            logger.error("[ERR-033] Failed to determine local IP address: {}", e.getMessage(), e);
            return "";
        }
    }

    /**
     * Get the local host name.
     *
     * @return The host name, or "unknown" if it can't be determined
     */
    public static String getHostname() {
        if (cachedHostname != null) {
            return cachedHostname;
        }

        try {
            InetAddress localHost = InetAddress.getLocalHost();
            cachedHostname = localHost.getHostName();
            return cachedHostname;
        } catch (UnknownHostException e) {
            logger.error("[ERR-091] Failed to determine local hostname: {}", e.getMessage(), e);
            return "";
        }
    }
}
