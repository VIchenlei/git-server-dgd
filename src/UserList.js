export default class UserList {
  constructor () {
    this.list = new Map()
    this.loginTimeList = new Map()
  }

  add (user) {
    this.list.set(user.name, user)
  }

  remove (user) {
    this.list.delete(user.name)
  }

  get (userName) {
    return this.list.get(userName)
  }

  getList () {
    return this.list
  }
}
