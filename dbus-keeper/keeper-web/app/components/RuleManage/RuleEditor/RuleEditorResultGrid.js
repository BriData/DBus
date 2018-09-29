/**
 * Created by haowei6 on 2017/11/29.
 */
import React, {PropTypes, Component} from 'react'
import ReactDOM from 'react-dom'
import {Affix, Slider, Table} from 'antd'
import styles from './res/styles/index.less'

export default class RuleEditorResultGrid extends Component {

  componentDidMount = () => {
    let antdTable = ReactDOM.findDOMNode(this.refs.antdTable);
    for (let i = 0; i < 6; i++) antdTable = antdTable.childNodes[0];
    let self = this;
    antdTable.onscroll = function () {
      let antdSlider = ReactDOM.findDOMNode(self.refs.antdSlider);
      let track = antdSlider.childNodes[1];
      let Role = antdSlider.childNodes[3];
      let max = antdTable.scrollWidth - antdTable.clientWidth;
      track.style.width = (antdTable.scrollLeft / max * 100) + '%';
      Role.style.left = (antdTable.scrollLeft / max * 100) + '%';
    };
  }
  onSliderChange = (value) => {
    let antdTable = ReactDOM.findDOMNode(this.refs.antdTable);
    for (let i = 0; i < 6; i++) antdTable = antdTable.childNodes[0];
    let max = antdTable.scrollWidth - antdTable.clientWidth;
    antdTable.scrollLeft = value / 10000 * max;
  }

  render = () => {
    return (
      <div>
        <div className="col-xs-12">
          <Affix>
            <Slider className={styles.ruleResultSlider} ref="antdSlider" max={10000} onChange={this.onSliderChange} tipFormatter={null}/>
          </Affix>
        </div>
        <div className="col-xs-12">
          <Table ref="antdTable" rowClassName={() => {
            return "ant-table-row-custom-td"
          }}
                 locale={{emptyText: "No log"}}
                 pagination={false}
                 scroll={{x: true}}
                 bordered
                 size="default"
                 dataSource={this.props.resultSource}
                 columns={this.props.resultColumns}>
          </Table>
        </div>
      </div>
    )
  }
}
