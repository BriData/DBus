import React, { PropTypes, Component } from 'react'

export default class LoginCanvas extends Component {
  constructor (props, context) {
    super(props, context)
    this.flag = false
    this.canvas = null
    this.canvasContext = null
    this.circleArr = []
    this.WIDTH = 0
    this.HEIGHT = 0
    this.POINT = 40
  }

  componentDidMount () {
    // 定义画布宽高和生成点的个数
    this.WIDTH = window.innerWidth
    this.HEIGHT = window.innerHeight
    this.canvas = document.getElementById('Mycanvas')
    this.canvas.width = this.WIDTH
    this.canvas.height = this.HEIGHT
    this.canvasContext = this.canvas.getContext('2d')
    this.canvasContext.strokeStyle = 'rgba(255,255,255,0.5)'
    this.canvasContext.strokeWidth = 1
    this.canvasContext.fillStyle = 'rgba(255,255,255,0.2)'

    // 初始化
    this.init(this.canvasContext, this.canvas, this.HEIGHT, this.WIDTH)
    this.Interval = setInterval(() => {
      for (let i = 0; i < this.POINT; i++) {
        const cir = this.circleArr[i]
        cir.x += cir.moveX
        cir.y += cir.moveY
        if (cir.x > this.WIDTH) cir.x = 0
        else if (cir.x < 0) cir.x = this.WIDTH
        if (cir.y > this.HEIGHT) cir.y = 0
        else if (cir.y < 0) cir.y = this.HEIGHT
      }
      this.draw(this.canvasContext, this.canvas, this.circleArr)
    }, 16)
    document.getElementById('loginContainer').addEventListener('mousemove', e => {
      for (let i = 0; i < this.POINT; i++) {
        this.circleArr[i].x += e.movementX * (i * 2 - this.POINT) / this.POINT
        this.circleArr[i].y += e.movementY * (i * 2 - this.POINT) / this.POINT
      }
    })
  }
  componentWillUnmount () {
    clearInterval(this.Interval)
  }
 // 线条：开始xy坐标，结束xy坐标，线条透明度
  Line (x, y, _x, _y, o) {
    this.beginX = x
    this.beginY = y
    this.closeX = _x
    this.closeY = _y
    this.o = o
  }
	// 点：圆心xy坐标，半径，每帧移动xy的距离
  Circle (x, y, r, moveX, moveY) {
    this.x = x
    this.y = y
    this.r = r
    this.moveX = moveX
    this.moveY = moveY
  }
	// 生成max和min之间的随机数
  num=(max, _min) => {
    const min = arguments[1] || 0
    return Math.floor(Math.random() * (max - min + 1) + min)
  }
	// 绘制原点
  drawCricle=(cxt, x, y, r, moveX, moveY) => {
    const circle = new this.Circle(x, y, r, moveX, moveY)
    cxt.beginPath()
    cxt.arc(circle.x, circle.y, circle.r, 0, 2 * Math.PI)
    cxt.closePath()
    cxt.fill()
    return circle
  }
	// 绘制线条
  drawLine=(cxt, x, y, _x, _y, o) => {
    const line = new this.Line(x, y, _x, _y, o)
    cxt.beginPath()
    cxt.strokeStyle = `rgba(255,255,255,${o})`
    cxt.moveTo(line.beginX, line.beginY)
    cxt.lineTo(line.closeX, line.closeY)
    cxt.closePath()
    cxt.stroke()
  }
  // 每帧绘制
  draw=(context, canvas, circleArr) => {
    context.clearRect(0, 0, canvas.width, canvas.height)
    for (var i = 0; i < this.POINT; i++) {
      this.drawCricle(context, circleArr[i].x, circleArr[i].y, circleArr[i].r)
    }
    for (var i = 0; i < this.POINT; i++) {
      for (var j = 0; j < this.POINT; j++) {
        if (i + j < this.POINT) {
          var A = Math.abs(circleArr[i + j].x - circleArr[i].x),
            B = Math.abs(circleArr[i + j].y - circleArr[i].y)
          var lineLength = Math.sqrt(A * A + B * B)
          var C = 1 / lineLength * 7 - 0.009
          var lineOpacity = C > 0.03 ? 0.03 : C
          if (lineOpacity > 0) {
            this.drawLine(context, circleArr[i].x, circleArr[i].y, circleArr[i + j].x, circleArr[i + j].y, lineOpacity)
          }
        }
      }
    }
  }
 // 初始化生成原点
  init=(context, canvas, HEIGHT, WIDTH) => {
    this.circleArr = []
    for (var i = 0; i < this.POINT; i++) {
      this.circleArr.push(this.drawCricle(context, this.num(WIDTH), this.num(HEIGHT), this.num(15, 2), this.num(10, -10) / 40, this.num(10, -10) / 40))
    }
    this.draw(context, canvas, this.circleArr)
  }
  render () {
    return (
      <canvas id="Mycanvas" style={{position: 'absolute', top: '0px', left: '0px', right: '0px', bottom: '0px'}}></canvas>
    )
  }
}
