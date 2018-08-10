/**
 * Created by haowei6 on 2017/10/11.
 */

import React, {PropTypes, Component} from 'react'
import EditRuleGrammarLine from './EditRuleGrammarLine'

export default class EditRuleGrammar extends Component {

  constructor(props) {
    super(props)
    this.state = {}
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  generateRuleGrammar = () => ({
    key: this.handleRandom('grammar'),
    ruleParamter: null,
    ruleOperate: null,
    ruleScope: null,
    name: null,
    type: null,
    ruleType: null,
    subStartType: null,
    subStart: null,
    subEndType: null,
    subEnd: null,
    filterKey: null
  })

  autoFillField = (rule, no) => {
    if (rule.ruleTypeName !== 'jsonPath') return
    const ruleGrammars = rule.ruleGrammar
    if (!ruleGrammars || ruleGrammars.length < 2) return
    ruleGrammars[no + 1].ruleScope = ruleGrammars[no].ruleScope
    ruleGrammars[no + 1].ruleParamter = ruleGrammars[no].ruleParamter
  }

  arrangeToIndexOrder = (rule) => {
    if (rule.ruleTypeName !== 'toIndex') return
    const ruleGrammars = rule.ruleGrammar
    for (var i = 0; i < ruleGrammars.length; i++) {
      ruleGrammars[i].ruleScope = '' + i;
    }
  }

  handleAdd = (no) => {
    const {rule, onRuleGrammarChange} = this.props
    const ruleGrammars = rule.ruleGrammar
    ruleGrammars.splice(no + 1, 0, this.generateRuleGrammar());
    this.autoFillField(rule, no)
    this.arrangeToIndexOrder(rule);
    onRuleGrammarChange(rule)
  }

  handleDelete = (no) => {
    const {rule, onRuleGrammarChange} = this.props
    const ruleGrammars = rule.ruleGrammar
    if (ruleGrammars.length <= 1) return
    ruleGrammars.splice(no, 1)
    this.arrangeToIndexOrder(rule)
    onRuleGrammarChange(rule)
  }

  handleMoveUp = (no) => {
    if (no === 0) return
    const {rule, onRuleGrammarChange} = this.props
    const {ruleGrammars} = rule
      [ruleGrammars[no], ruleGrammars[no - 1]] = [ruleGrammars[no - 1], ruleGrammars[no]];
    this.arrangeToIndexOrder(rule);
    onRuleGrammarChange(rule)
  }
  handleMoveDown = (no) => {
    const {rule, onRuleGrammarChange} = this.props
    const {ruleGrammars} = rule
    if (no === ruleGrammars.length - 1) return
    [ruleGrammars[no], ruleGrammars[no + 1]] = [ruleGrammars[no + 1], ruleGrammars[no]];
    this.arrangeToIndexOrder(rule)
    onRuleGrammarChange(rule)
  }

  render = () => {
    const {rule} = this.props
    const ruleGrammarList = rule.ruleGrammar || []
    let lines = [];
    for (let i = 0; i < ruleGrammarList.length; i++) {
      let ruleGrammar = ruleGrammarList[i];
      lines.push(
        <EditRuleGrammarLine
          key={ruleGrammar.key}
          ruleGrammar={ruleGrammar}
          ruleType={rule.ruleTypeName}
          no={i}
          onAdd={this.handleAdd}
          onDelete={this.handleDelete}
          onMoveUp={this.handleMoveUp}
          onMoveDown={this.handleMoveDown}
        />)
    }
    return (
      <div>
        {lines}
      </div>
    );
  }
}
