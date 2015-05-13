Reads from a beanstalk queue, calls webhooks with notifications and returns the result to a beanstalk queue.

## Requirements

Requires a running beanstalkd http://kr.github.io/beanstalkd/

## Installation

```
git clone https://github.com/tokenly/xcaller
cd xcaller/bin
npm install
```

## Usage

`BEANSTALK_HOST=127.0.0.1 DEBUG=1 node xcaller.js`
