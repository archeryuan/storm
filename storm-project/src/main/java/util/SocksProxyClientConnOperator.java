package util;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.OperatedClientConnection;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.impl.conn.DefaultClientConnectionOperator;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

public class SocksProxyClientConnOperator extends DefaultClientConnectionOperator {
	public SocksProxyClientConnOperator(final SchemeRegistry schemes) {
		super(schemes);
	}

	@Override
	public void openConnection(final OperatedClientConnection conn, final HttpHost target, final InetAddress local,
								final HttpContext context, final HttpParams params) throws IOException {
		if (conn == null) {
			throw new IllegalArgumentException("Connection may not be null");
		}
		if (target == null) {
			throw new IllegalArgumentException("Target host may not be null");
		}
		if (params == null) {
			throw new IllegalArgumentException("Parameters may not be null");
		}
		if (conn.isOpen()) {
			throw new IllegalStateException("Connection must not be open");
		}

		SchemeSocketFactory socksPlainSocketFactory = new SocksPlainSocketFactory();

		Scheme schm = schemeRegistry.getScheme(target.getSchemeName());
		SchemeSocketFactory sf = schm.getSchemeSocketFactory();

		InetAddress[] addresses = resolveHostname(target.getHostName());
		int port = schm.resolvePort(target.getPort());

		Socket sock = socksPlainSocketFactory.createSocket(params);
		conn.opening(sock, target);

		for (int i = 0; i < addresses.length; i++) {
			InetAddress address = addresses[i];
			boolean last = i == addresses.length - 1;
			InetSocketAddress remoteAddress = new InetSocketAddress(address, port);
			InetSocketAddress localAddress = null;
			if (local != null) {
				localAddress = new InetSocketAddress(local, 0);
			}
			try {
				Socket connsock = sf.connectSocket(sock, remoteAddress, localAddress, params);
				if (sock != connsock) {
					sock = connsock;
					conn.opening(sock, target);
				}
				prepareSocket(sock, context, params);
				conn.openCompleted(sf.isSecure(sock), params);
				break;
			} catch (ConnectException ex) {
				if (last) {
					throw new HttpHostConnectException(target, ex);
				}
			} catch (ConnectTimeoutException ex) {
				if (last) {
					throw ex;
				}
			}
		}
	}
}
