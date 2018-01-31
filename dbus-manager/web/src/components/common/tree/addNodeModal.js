/**
 * Created by ximeiwang on 2017/7/24.
 */
import { Modal, Form, Input } from 'antd';
var React = require('react');
const FormItem = Form.Item;
var store = require('./zk-tree-store');

const NodeCreateForm = Form.create()(
    (props) => {
        const { visible, onCancel, onCreate, form } = props;
        const { getFieldDecorator } = form;
        return (
            <Modal
                visible={visible}
                title="Create a new node"
                okText="Create"
                cancelText="Cancel"
                onCancel={onCancel}
                onOk={onCreate}
            >
                <Form layout="vertical">
                    <FormItem label="Node Name">
                        {getFieldDecorator('title', {
                            rules: [{ required: true, message: 'Please input the name of node!' }],
                        })(
                            <Input onPressEnter={onCreate}/>
                        )}
                    </FormItem>
                </Form>
            </Modal>
        );
    }
);
const CreateNodePage = React.createClass({
    getInitialState: function() {
        return {
            visible: false,
        }
    },
    showModal: function(){
        this.setState({ visible: true});
    },
    handleCancel: function(){
        this.setState({ visible: false });
    },
    handleCreate: function(){
        const form = this.form;
        form.validateFields((err, values) => {
            if (err) {
                return;
            }
            this.props.onSave(values.title);
            form.resetFields();
            this.setState({ visible: false });
        });
    },
    saveFormRef: function(form){
        this.form = form;
    },
    render() {
        return (
            <div>
                <NodeCreateForm
                    ref={this.saveFormRef}
                    visible={this.state.visible}
                    onCancel={this.handleCancel}
                    onCreate={this.handleCreate}
                />
            </div>
        );
    }
});

module.exports = CreateNodePage;
