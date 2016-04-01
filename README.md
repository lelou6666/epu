# Elastic Processing Unit (EPU) Services and Agents for Phantom

This repository contains the Elastic Processing Unit (EPU) Services and
Agents that form the core components of [Phantom](http://www.nimbusproject.org/doc/phantom/latest/).

There are four main components:

* the DTRS (deployable type registry service)
* the Provisioner
* the EPUM (EPU Management)
* the Process Dispatcher

## Installation

To install the EPU services for development purpose, you will need Python 2.7 and a RabbitMQ server.

After cloning this repository, run the following command, preferably inside a dedicated virtualenv:

    pip install -r requirements.txt --allow-external dashi --allow-unverified dashi .

You should then be able to start services via the following executables:

    epu-dtrs
    epu-provisioner-service
    epu-management-service
    epu-processdispatcher-service

By default services will communicate using the following RabbitMQ settings:

    host: localhost
    port: 5672
    username: guest
    password: guest
    vhost: /
    heartbeat: 30
    exchange: default_dashi_exchange

You can use the [ceiclient command line tool](https://github.com/nimbusproject/ceiclient) to communicate with these services.