const React = require('react');
const antd = require('antd');
const Steps = antd.Steps;
const Step = Steps.Step;

var StepCursor = React.createClass({
    getDefaultProps: function() {
        return {
            currentStep: 0
        }
    },
    render: function() {
        return (
            <Steps current={this.props.currentStep}>
                <Step title="数据源基本信息" />
                <Step title="选择Schema" />
                <Step title="修改ZK配置" />
                <Step title="启动Topology" />
            </Steps>
        );
    }
});

module.exports = StepCursor;
