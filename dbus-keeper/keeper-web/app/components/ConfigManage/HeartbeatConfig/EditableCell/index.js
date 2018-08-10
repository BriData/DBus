import React, {PropTypes, Component} from 'react'
import {Select, message,Input, Icon} from 'antd'
const Option = Select.Option

export default class EditableCell extends Component {
  constructor(props) {
    super(props)

    const {value, index, param} = props
    this.state = {
      isEdit: false,
      value,
      index,
      param
    }
    this.input = null
  }

  componentWillReceiveProps = nextProps => {
    const {value, index, param} = nextProps
    this.setState({value, index, param})
  }

  handleSaveInputValue = () => {

    const {index, param, value} = this.state
    if (param === 'startTime' || param === 'endTime') {
      if (!value.match(/^(\d|[0-1]\d|2[0123]):(\d|[0-5]\d)$/)) {
        message.error('时间格式不正确');
        return
      }
    }

    const {onSave} = this.props
    onSave(index, param, value)
    this.setState({
      isEdit: false
    })

  }

  focusInput = () => {
    if (!this.input) return
    const input = this.input.refs.input;
    input.focus();
    input.setSelectionRange(0, input.value.length);
  }

  render() {
    const {isEdit, value, param} = this.state
    const {allDataSchemaList} = this.props
    return (
      <div>
        {isEdit
          ?
          param === 'schema'
            ?
            (<Select
              showSearch
              optionFilterProp='children'
              style={{width: 180}}
              onSelect={value => this.setState({value})}
              onBlur={this.handleSaveInputValue}
              value={value}
            >
              {allDataSchemaList.map(item => (
                <Option value={`${item.dsName}/${item.schemaName}`} key={`${item.dsName}/${item.schemaName}`}>
                  {`${item.dsName}/${item.schemaName}`}
                </Option>
              ))}
            </Select>)
            :
            (<Input
              ref={input => this.input = input}
              value={value}
              onBlur={this.handleSaveInputValue}
              onPressEnter={this.handleSaveInputValue}
              onChange={e => this.setState({value: e.target.value})}
            />)
          :
          <span>
            {value}
            <Icon
              type="edit"
              style={{fontSize: 14}}
              onClick={() => this.setState({isEdit: true}, this.focusInput)}
            />
          </span>
        }
      </div>
    )
  }
}

EditableCell.propTypes = {}
