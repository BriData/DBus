import { createActionTypes } from "@/app/utils/createAction"

/**
 * @author xiancangao
 * @description redux->type
 */

export const GLOBAL_FULL_PULL = createActionTypes('toolSet/ControlMessage/GLOBAL_FULL_PULL', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

