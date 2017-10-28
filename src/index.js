/* @flow */
import BPromise from 'bluebird'
import mongoose from 'mongoose'
import EventEmitter from 'events'

mongoose.Promise = BPromise

type MongoSMQ$options = {
    host?: string,
    db?: string,
    port?: number,
    options?: mixed,
    client?: ?string,
    ns?: string,
    visibility?: number,
    colName: string,
};

type MongoSMQ$message = {
  _id : Object,
  tries : ?number,
  message : any,
  visible : ?Object
};

type MongoSMQ$updatePayload = {
  _id : string,
  tries : ?number,
  message : any
};

function now () {
  return (new Date()).toISOString()
}

function nowPlusSecs (secs: number) {
  return (new Date(Date.now() + (secs * 1000))).toISOString()
}

const { Schema } = mongoose
const MessageSchema = new Schema({
  message: Schema.Types.Mixed,
  visible: Schema.Types.Date,
  ack: Schema.Types.String,
  tries: Schema.Types.Number
}, {
  timestamps: true
})
export default class MongoSMQ extends EventEmitter {
  options: MongoSMQ$options;
  mongo: Mongoose$Connection;
  Message: Class<Mongoose$Model>;

  constructor (options: MongoSMQ$options = {}) {
    super()
    const opts = Object.assign({}, {
      host: 'localhost',
      db: 'mongoSMQ',
      port: 27017,
      options: {},
      client: null,
      ns: 'rsmq',
      visibility: 30,
      colName: 'SMQMessage'
    }, options)
    this.options = opts
  }

  async init () {
    const {
      host = '', port = '', db = '', colName
    } = this.options
    let theConnection: Mongoose$Connection = await mongoose.createConnection(
      `mongodb://${host}:${port}/${db}`,
      {
        useMongoClient: true
      }
    )
    if (theConnection) {
      this.mongo = theConnection
      this.Message = this.mongo.model(colName, MessageSchema)
    }
    return this
  }

  deinit (): Promise<Mongoose$Connection> {
    return this.mongo.close()
  }

  createMessage (payload: mixed): Promise<MongoSMQ$message> {
    const { Message } = this
    const newMsg = new Message({
      message: payload,
      visible: now()
    })
    return newMsg.save()
  }

  getMessage (payload: any, opts: ?{visibility: number}): Promise<MongoSMQ$message> {
    const { Message } = this
    const visibility = (opts && opts.visibility !== undefined)
      ? opts.visibility : this.options.visibility
    const query = Object.assign({
      visible: { $lte: now() }
    }, payload)
    const sort = {
      _id: 1,
      visible: 1
    }
    const update = {
      $inc: { tries: 1 },
      $set: {
        visible: nowPlusSecs(visibility || 0)
      }
    }
    return Message.findOneAndUpdate(query, update, { sort, new: true }).then()
  }

  updateMessage (query: {_id : string, tries? : ?number},
    update: any): Promise<MongoSMQ$message> {
    const { Message } = this
    return Message.findOneAndUpdate(query, update, { new: true }).then()
  }

  removeMessageById ({ _id, tries }: { _id: string, tries?: ?number }): Promise<mixed> {
    const { Message } = this
    const query = {
      _id,
      tries
    }
    /* For tries value,
    ** If it null we mean we looking for object with tries property is null or not exist
    ** If it undefined we mean we don't care about value of tries when find
    */
    if (tries === undefined) {
      delete query.tries
    }
    return Message.findOneAndRemove(query).then()
  }

  total (): Promise<number> {
    const { Message } = this
    return Message.count().then()
  }

  size (): Promise<number> {
    const { Message } = this
    const query = {
      visible: { $lte: now() }
    }
    return Message.count(query).then()
  }

  inFlight (): Promise<number> {
    const { Message } = this
    const query = {
      tries: { $exists: true },
      visible: { $gt: now() }
    }
    return Message.count(query).then()
  }

  clean (): Promise<{ "acknowledged" : boolean, "deletedCount" : number }> {
    const { Message } = this
    return Message.deleteMany().then()
  }
}
