/**
* Copyright 2014-2015 Smartsheet.com, Inc.
* 
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
*
*/

/**
 * Wrapper around RabbitMQ.
 * Simplify publishing to the message queue.
 * Assumes a single Connection, with a single Channel, publishing to a single
 * Exchange.
 */
package com.smartsheet.tin.filters.pkpublish;

import java.io.IOException;

import org.apache.log4j.Logger;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author scottw
 *
 */
public class MQPublishWrapper {
	private static Logger logger = Logger.getLogger(MQPublishWrapper.class);

	public class MQError extends Exception {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public MQError(String message) {
			super(message);
		}
	}

	public class MQConfig {
		private ConnectionFactory mqFactory;
		protected int recoveryInterval;
		protected int retryLimit;
		protected String exchangeName;
		protected String exchangeType;
		protected boolean exchangeIsDurable;
		protected boolean isReady;
		protected int closeTimeout = 10;
		protected boolean haveHost;

		public MQConfig() {
			this.mqFactory = new ConnectionFactory();
			this.recoveryInterval = 0;
			this.exchangeName = "";
			this.exchangeType = "direct";
			this.exchangeIsDurable = false;
			this.isReady = false;
		}

		public void setHost(String host) {
			this.mqFactory.setHost(host);
			this.haveHost = true;
		}

		public void setPort(int port) {
			this.mqFactory.setPort(port);
		}

		public void setVirtualHost(String vhost) {
			this.mqFactory.setVirtualHost(vhost);
		}

		public void setUserName(String userName) {
			this.mqFactory.setUsername(userName);
		}

		public void setPassword(String password) {
			this.mqFactory.setPassword(password);
		}

		public void setAutomaticRecoveryEnabled(boolean automaticRecovery) {
			this.mqFactory.setAutomaticRecoveryEnabled(automaticRecovery);
		}

		/**
		 * The network recovery interval is used both internally by
		 * rabbitMQ and for timeouts when reconnecting or attempting to
		 * republish messages.
		 * 
		 * @param recoveryInterval  in milliseconds
		 */
		public void setNetworkRecoveryInterval(int recoveryInterval) {
			this.recoveryInterval = recoveryInterval;
			this.mqFactory.setNetworkRecoveryInterval(recoveryInterval);
		}

		/**
		 * The heartbeat interval, in seconds.
		 * @param heartbeatInterval seconds between heartbeat messages.
		 */
		public void setHeartBeatInterval(int heartbeatInterval) {
			this.mqFactory.setRequestedHeartbeat(heartbeatInterval);
		}

		public void setRetryLimit(int retryLimit) {
			this.retryLimit = retryLimit;
		}

		public String toString() {
			return String.format("<MQConfig host: '%s', port: %d, " +
					"vhost: '%s', exchange: '%s', exchangeType: '%s'>",
					this.mqFactory.getHost(), this.mqFactory.getPort(),
					this.mqFactory.getVirtualHost(), this.exchangeName,
					this.exchangeType);
		}


	}

	protected MQConfig config;

	private Connection mqConnection;
	private Channel mqChannel;
	private boolean mqReady;


	public MQPublishWrapper() {
		this.config = new MQConfig();
	}

	/**
	 * Callers call this function to mark that the configuration is complete.
	 * Attempts to publish to the message queue prior to this call will fail.
	 */
	public void markConfigComplete() {
		if (! this.config.haveHost) {
			logger.warn("Using 'localhost' as the message queue server");
		}
		this.config.isReady = true;
	}

	/**
	 * Connect to a message queue, after the config is complete.
	 * @NOTE: must be called after .markConfigComplete().
	 * 
	 * @param exchangeName
	 * @param exchangeType
	 * @param durable
	 * @throws MQError
	 */
	public void connect(String exchangeName, String exchangeType,
			boolean durable) throws MQError {
		if (! this.config.isReady) {
			logger.error("Do not prepare before config complete.");
			throw new MQError("prepareMQ() before markConfigComplete().");
		}
		this.config.exchangeName = exchangeName;
		this.config.exchangeType = exchangeType;
		this.config.exchangeIsDurable = durable;
		for (int i = 0; i < this.config.retryLimit + 1; ++i) {
			try {
				this.reconnect();
				logger.info("Connected to MQ: " + this.config.toString());
				return;
			} catch (MQError e) {
				logger.warn("Try [" + i + "] preparing message queue failed.");
			}
		}
		throw new MQError(
				String.format("Failed to connect to message queue: %s",
						this.config.toString()));
	}

	/**
	 * Connect using already configured exchange info.
	 * @throws MQError
	 */
	public void connect() throws MQError {
		this.connect(this.config.exchangeName, this.config.exchangeType,
				this.config.exchangeIsDurable);
	}

	/**
	 * Reconnect to a message queue, in the event of a lost connection.
	 * This method should only be used with a configured MQPublishWrapper.
	 * 
	 * Swallows all errors except being called before the config is ready.
	 * @throws MQError 
	 */
	public void reconnect() throws MQError {
		if (! this.config.isReady) {
			logger.error("Do not prepare before config complete.");
			throw new MQError("prepareMQ() before markConfigComplete().");
		}
		try {
			if (! this.mqReady) {
				this.releaseMQ();
			}
			this.mqConnection = this.config.mqFactory.newConnection();
			this.mqChannel = this.mqConnection.createChannel();
			this.mqChannel.exchangeDeclare(this.config.exchangeName,
					this.config.exchangeType,
					this.config.exchangeIsDurable);
			this.mqReady = true;
			logger.debug("Message queue connected: " + this.config.toString());
			return;
		} catch (IOException e) {
			logger.warn("Error connecting to message queue:", e);
		}
		try {
			// NOTE: Sleeping here results in an extra sleep.
			// Sleeping here simplifies the callers, so it's worth it.
			Thread.sleep(this.config.recoveryInterval);
		} catch (InterruptedException e) {
			logger.warn("Sleep interrupted:", e);
		}
		throw new MQError("Unable to connect to message queue at: " + this.config.toString());
	}

	public void releaseMQ() {
		if (this.mqChannel != null) {
			try {
				this.mqChannel.close();
			} catch (IOException e) {
				logger.warn("Channel.close() failed in .releaseMQ:", e);
			}
		}

		if (this.mqConnection != null && this.mqConnection.isOpen()) {
			try {
				this.mqConnection.close(this.config.closeTimeout);
			} catch (IOException e) {
				logger.warn("Connection.close failed in .releaseMQ:", e);
			}
		}
		this.mqReady = false;
	}

	public void publishMessage(String routingKey, String msg) throws MQError {
		for (int i = 0; i < this.config.retryLimit + 1; ++i) {
			try {
				if (! this.mqReady) {
					logger.warn(".publishMessage() called before ready.");
					this.reconnect();
				}
				this.mqChannel.basicPublish(this.config.exchangeName,
						routingKey, null, msg.getBytes());
				logger.debug(String.format(
						"Message published to Exchange: '%s', " +
								"routingKey: '%s' msg: '%s'",
								this.config.exchangeName, routingKey, msg));
				return;
			} catch (IOException e) {
				logger.warn("Temporary failure publishing message.", e);
			}
			catch (com.rabbitmq.client.AlreadyClosedException e) {
				String err = "Message queue close when publishing:" +
						e.toString();
				logger.error(err, e);
				throw new MQError(err);
			}
		}
		throw new MQError(String.format("Failed to publish message to " +
				"Exchange: '%s', routingKey: '%s', msg: '%s'",
				this.config.exchangeName, routingKey, msg));
	}
}
