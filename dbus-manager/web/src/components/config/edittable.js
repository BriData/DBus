import {Table, Input, Select,Icon, Button, Popconfirm,} from 'antd';

const Option = Select.Option;
var React = require('react');
var B = require('react-bootstrap');
var Col = B.Col;

class EditableCell_use extends React.Component {
    state = {
        value: this.props.value,
        editable: false,
    }
    handleChange = (value) => {
        console.log(`selected ${value}`);
        // const value = e.target.value;
        this.setState({value});
    }
    check = () => {
        this.setState({editable: false});
        if (this.props.onChange) {
            this.props.onChange(this.state.value);
        }
    }
    edit = () => {
        this.setState({editable: true});
    }

    render() {
        const {value, editable} = this.state;
        return (
            <div  className="editable-cell" >
                {
                    editable ?
                        <div className="editable-cell-input-wrapper">
                            <Select
                                value = {value}
                                onChange = {this.handleChange}
                                onPressEnter={this.check}>
                                <Option value="N">N</Option>
                                <Option value="Y">Y</Option>
                            </Select>
                            <Icon
                                type="check"
                                className="editable-cell-icon-check"
                                style={{ fontSize: 24, color: '#08c' }}
                                onClick={this.check}
                            />
                        </div>
                        :
                        <div className="editable-cell-text-wrapper">
                            {value || ' '}
                            <Icon
                                type="edit"
                                className="editable-cell-icon"
                                style={{ fontSize: 18 }}
                                onClick={this.edit}
                            />
                        </div>
                }
            </div>
        );
    }
}

class EditableCell_tabel extends React.Component {
    state = {
        value: this.props.value,
        editable: false,
        dslist: this.props.dslist,
        schema: this.props.schema,
    }
    handleChange = (value) => {
        console.log(`selected ${value}`);
        // const value = e.target.value;
        this.setState({value});
    }
    check = () => {
        this.setState({editable: false});
        if (this.props.onChange) {
            let tabel = this.state.value;
             if(tabel ==""){
                alert("表名不能为空");
                this.props.onChange("000");
                this.setState({ editable: true });
                return false;
            }
            if(!tabel.match(/^([a-zA-Z0-9_-])+\/([a-zA-Z0-9_-])+$/)){
                alert("表名格式不正确");
                this.props.onChange("000");
                this.setState({ editable: true });
                return false;
            }
            // this.state.dslist.findIndex(tabel) == -1
            let dslist = this.state.dslist;
            let schema = this.state.schema;
            let list_dsName = [];
            for (var key in dslist){
                list_dsName.push(dslist[key].dsName);
            }
            let ds_schema = tabel.split('/',2);
            let ds = ds_schema[0];
            if(list_dsName.findIndex(x => x == ds) == -1){
                alert("dsName不是数据源中的dsName");
                this.props.onChange("000");
                this.setState({ editable: true });
                return false;
            }
            let list_schemaName = schema;
            if(list_schemaName.findIndex(x => x == tabel) == -1){
                alert("schemaName不是数据源中的schemaName");
                this.props.onChange("000");
                this.setState({ editable: true });
                return false;
            }
            this.props.onChange(this.state.value);
        }
    }
    createList = () => {
        var list = [];
        let schema = this.state.schema;
        console.info('schemacreateListcreateListcreateList');
        console.info(schema);
        for (var i = 0;i<schema.length;i++){
            var value_this = schema[i];
            list.push(<option value={value_this} >{value_this}</option>);
        }
        return list;
    }
    edit = () => {
        this.setState({editable: true});
    }
    render() {
        const {value, editable} = this.state;
        return (
            <div  className="editable-cell" >
                {
                    editable ?
                        <div className="editable-cell-input-wrapper">
                            <Select
                                showSearch
                                value = {value}
                                onChange = {this.handleChange}
                                onPressEnter={this.check}
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}>
                                <option value="-1" >select schema</option>
                                { this.createList() }
                            </Select>
                            <Icon
                                type="check"
                                className="editable-cell-icon-check"
                                style={{ fontSize: 24, color: '#08c' }}
                                onClick={this.check}
                            />
                        </div>
                        :
                        <div className="editable-cell-text-wrapper">
                            {value || ' '}
                            <Icon
                                type="edit"
                                className="editable-cell-icon"
                                style={{ fontSize: 18 }}
                                onClick={this.edit}
                            />
                        </div>
                }
            </div>
        );
    }
}

class EditableCell_email extends React.Component {
    state = {
        value: this.props.value,
        editable: false,
    }
    handleChange = (e) => {
        const value = e.target.value;
        this.setState({value});
    }
    check = () => {
        this.setState({editable: false});
        if (this.props.onChange) {
            let email = this.state.value;
             if(email ==""){
                alert("邮箱不能为空");
                this.props.onChange("000");
                this.setState({ editable: true });
                return false;
            }
            this.props.onChange(this.state.value);
        }
    }
    edit = () => {
        this.setState({editable: true});
    }



    render() {
        const {value, editable} = this.state;
        return (
            <div  className="editable-cell" >
                {
                    editable ?
                        <div className="editable-cell-input-wrapper">
                            <Input
                                value={value}
                                onChange={this.handleChange}
                                onPressEnter={this.check}
                            />
                            <Icon
                                type="check"
                                className="editable-cell-icon-check"
                                style={{ fontSize: 24, color: '#08c' }}
                                onClick={this.check}
                            />
                        </div>
                        :
                        <div className="editable-cell-text-wrapper">
                            {value || ' '}
                            <Icon
                                type="edit"
                                className="editable-cell-icon"
                                style={{ fontSize: 18 }}
                                onClick={this.edit}
                            />
                        </div>
                }
            </div>
        );
    }
}

class EditableCell extends React.Component {
    state = {
        value: this.props.value,
        editable: false,
    }
    handleChange = (e) => {
        const value = e.target.value;
        this.setState({value});
    }
    check = () => {
        this.setState({editable: false});
        if (this.props.onChange) {
            this.props.onChange(this.state.value);
        }
    }
    edit = () => {
        this.setState({editable: true});
    }

    render() {
        const {value, editable} = this.state;
        return (
            <div  className="editable-cell" >
                {
                    editable ?
                        <div className="editable-cell-input-wrapper">
                            <Input
                                value={value}
                                onChange={this.handleChange}
                                onPressEnter={this.check}
                            />
                            <Icon
                                type="check"
                                className="editable-cell-icon-check"
                                style={{ fontSize: 24, color: '#08c' }}
                                onClick={this.check}
                            />
                        </div>
                        :
                        <div className="editable-cell-text-wrapper">
                            {value || ' '}
                            <Icon
                                type="edit"
                                className="editable-cell-icon"
                                style={{ fontSize: 18 }}
                                onClick={this.edit}
                            />
                        </div>
                }
            </div>
        );
    }
}
class EditableTable_1 extends React.Component {
    constructor(props) {
        super(props);
        this.columns = [{
            title: 'schemaName',
            dataIndex: 'schemaName',
            width: '22%',
            render: (text, record, index) => (
                <EditableCell_tabel
                    value={text}
                    dslist = {this.state.dslist}
                    schema = {this.state.schema}
                    onChange={this.onCellChange(index, 'schemaName')}
                />
            ),
        },  {
            title: 'Email',
            dataIndex: 'Email',
            width: '30%',
            render: (text, record, index) => (
                <EditableCell_email
                    value={text}
                    onChange={this.onCellChange(index, 'Email')}
                />
            ),
        }, {
            title: 'UseEmail',
            dataIndex: 'UseEmail',
            width: '10%',
            render: (text, record, index) => (
                <EditableCell_use
                    value={text}
                    onChange={this.onCellChange(index, 'UseEmail')}
                />
            ),
        }, {
            title: 'SMSNo',
            dataIndex: 'SMSNo',
            width: '18%',
            render: (text, record, index) => (
                <EditableCell
                    value={text}
                    onChange={this.onCellChange(index, 'SMSNo')}
                />
            ),
        }, {
            title: 'UseSMS',
            dataIndex: 'UseSMS',
            width: '10%',
            render: (text, record, index) => (
                <EditableCell_use
                    value={text}
                    onChange={this.onCellChange(index, 'UseSMS')}
                />
            ),
        },{
            title: 'ruleOperation',
            dataIndex: 'ruleOperation',
            width: '10%',
            render: (text, record, index) => {
                return (
                    this.state.dataSource.length > 0?
                        (
                            <Popconfirm title="Sure to delete?" onConfirm={() => this.onDelete(index)}>
                                <Icon type="minus-circle-o"
                                      style={{ fontSize: 22}}
                                />
                            </Popconfirm>
                        ) : null
                );
            },
        }];
        this.state = {
            dataSource: [],
            count: 0,
            dslist:[],
            schema:[],
        };
    }

    componentWillReceiveProps = (props) => {
        var dataSource_temp = props.dataSource;
        var dslist = props.dslist;
        var schema = props.schema;
        var dataSource_1 = [];
        var dataSource = [];
        var count = this.state.count;
        for (var key in dataSource_temp){
            var dataSource_ele = {};
            dataSource_ele.key = count;
            dataSource_ele.schemaName = key;
            dataSource_ele.SMSNo = dataSource_temp[key].SMSNo;
            dataSource_ele.UseSMS = dataSource_temp[key].UseSMS;
            dataSource_ele.Email = dataSource_temp[key].Email;
            dataSource_ele.UseEmail = dataSource_temp[key].UseEmail;
            dataSource_1 .push(dataSource_ele);
            dataSource = [...dataSource,dataSource_ele];
            count = count+1;
        };
        this.setState({
                dataSource: dataSource,
                count: count,
                dslist: dslist,
                schema: schema,
            });
    }
    onCellChange = (index, key) => {
        return (value) => {
            const dataSource = [...this.state.dataSource];
            dataSource[index][key] = value;
            this.setState({
                dataSource:dataSource,
            },() => this.props.callback_parent(this.state));
        };
    }
    onDelete = (index) => {
        const dataSource = [...this.state.dataSource];
        dataSource.splice(index, 1);
        this.setState({ dataSource },() => this.props.callback_parent(this.state));
    }
    handleAdd = () => {
        const {count, dataSource} = this.state;
        const newData = {
            key: count,
            schemaName:'datasource1/schema1',
            Email: " example@example.com",
            UseEmail:'Y',
            SMSNo: " 13800138000",
            UseSMS:'N',
        };
        this.setState({
            dataSource: [...dataSource, newData],
            count: count + 1,
        }, () => this.props.callback_parent(this.state));
    }

    render() {
        const {dataSource} = this.state;
        const columns = this.columns;
        return (
            <div>
                <Table bordered pagination={false} size = "small" dataSource={dataSource}  columns = {columns}/>
                <div className="row header">
                    <h4 className="col-xs-12">
                    </h4>
                </div>
                <Col smOffset={11} sm={2}>
                    <Button  className="editable-add-btn" onClick={this.handleAdd}>Add</Button>
                </Col>
            </div>
        );
    }
}

module.exports = EditableTable_1;
