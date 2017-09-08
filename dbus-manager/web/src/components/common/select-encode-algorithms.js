var React = require('react');

var Select = React.createClass({
    getInitialState: function() {
        return {
            options: [],
            defaultOpt: null,
            className: null,
            style:null
        };
    },
    createOptions: function(options) {
       var key = 0;
       var elems = [];
       options = options || [];
       options.forEach(function(o) {
           elems.push(<option key={++key} value={o.value}>{o.text}</option>);
       });
       return elems;
    },
    onChange: function() {
        if(this.props.onChange) {
            this.props.onChange(this.refs.select.value);
        }
    },
    getValue: function() {
        return this.refs.select?this.refs.select.value:null;
    },
    render: function() {
        return (
            <div className="ui-select" style={this.props.style} >
                <select ref="select"  onChange={ this.onChange } defaultValue={ this.props.defaultOpt } className={this.props.className}>
                    {this.createOptions(this.props.options)}
                </select>
            </div>
        );
    }
});

module.exports = Select;
