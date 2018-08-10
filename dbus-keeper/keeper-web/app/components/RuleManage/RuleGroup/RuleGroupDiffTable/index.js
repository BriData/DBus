import React, {PropTypes, Component} from 'react'
import styles from '../res/styles/index.less'
export default class RuleGroupDiffModal extends Component {

    createTitle = () => {
        let title = this.props.title;
        if (title.length === 0) return "Nothing to diff, please choose group";
        let subTitle = this.props.subTitle;
        let len = subTitle.length;
        return title.map(function (item) {
            return (<th colSpan={len}>
              <font size="4">{item}</font>
            </th>)
        });
    }
    createSubTitle = () => {
        let title = this.props.title;
        let subTitle = this.props.subTitle;
        return title.map(function (titleItem, titleIndex) {
            return subTitle.map(function (item, index) {
                if (index === subTitle.length - 1 && titleIndex !== title.length - 1) {
                  return (<th style={{borderRight:"#bbbbbb solid 1px"}}><font size="3">{item}</font></th>)
                }
              return (<th><font size="3">{item}</font></th>)
            })
        })
    }
    createContent = () => {
        let subTitle = this.props.subTitle;
        let content = this.props.content;

        let maxRow = ((content) => {
            let maxRow = 0;
            content.forEach(function (item) {
                if (item.length > maxRow) maxRow = item.length;
            });
            return maxRow;
        })(content);

        const checkRowDiff = (row) => {
            for (let i = 0; i < content.length - 1; i++) {
                let left = content[i][row];
                let right = content[i + 1][row];
                if (left == null && right == null) continue;
                if (left == null || right == null) return true;
                for (let j = 0; j < subTitle.length; j++) {
                    if (left[j] !== right[j]) return true;
                }
            }
            return false;
        };

        const drawTdInTr = function(row) {
            let ret = [];
            for (let part = 0; part < content.length; part++) {
                let current = content[part][row];
                for (let col = 0; col < subTitle.length; col++) {
                    let tdContent = <div>&nbsp;</div>;
                    if (current != null) {
                        tdContent = current[col];
                    }
                    let tag = (<td>{tdContent}</td>);
                    if (col === subTitle.length - 1 && part !== content.length - 1) {
                        tag = (<td style={{borderRight:"#bbbbbb solid 1px"}}>{tdContent}</td>);
                    }
                    ret.push(tag);
                }
            }
            return ret;
        };

        const diffRow = function (row) {
            return (<tr className={styles.danger}>{drawTdInTr(row)}</tr>);
        };
        const sameRow = function (row) {
            return (<tr>{drawTdInTr(row)}</tr>);
        };
        let ret = [];
        for (let row = 0; row < maxRow; row++) {
            if(checkRowDiff(row)) {
                ret.push(diffRow(row));
            } else {
                ret.push(sameRow(row));
            }
        }
        return ret;
    }

    render = () => {
        return (
            <div>
                <table
                  className={styles.diffTable}
                  cellPadding={0}
                  cellSpacing={0}
                  border="1"
                  width="100%"
                >
                    <thead>
                        <tr>
                          {this.createTitle()}
                        </tr>
                        <tr >
                          {this.createSubTitle()}
                        </tr>
                    </thead>
                    <tbody>
                        {this.createContent()}
                    </tbody>
                </table>
            </div>
        );
    }
}
RuleGroupDiffModal.propTypes = {}
