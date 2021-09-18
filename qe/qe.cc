
#include "qe.h"
#include "../rbf/rbfm.h"

#include <cmath>
#include <cstring>
#include <map>
#include <iostream>

using namespace std;


// Comparison functions for use in Filter and INLJoin --------------------------
static int compare(int32_t val1, int32_t val2)
{
    if (val1 < val2)
        return -1;
    else if (val1 > val2)
        return 1;
    return 0;
}

static int compare(float val1, float val2)
{
    if (val1 < val2)
        return -1;
    else if (val1 > val2)
        return 1;
    return 0;
}


static int compare(Attribute attr, const void *val1, const void *val2)
{
    int32_t s1, s2;
    int32_t i1, i2;
    float f1, f2;

    if (attr.type == TypeVarChar)
    {
        memcpy(&s1, val1, VARCHAR_LENGTH_SIZE);
        memcpy(&s2, val2, VARCHAR_LENGTH_SIZE);

        char str1[s1 + 1], str2[s2 + 1];
        str1[s1] = str2[s2] = '\0';

        memcpy(str1, (char*)val1 + VARCHAR_LENGTH_SIZE, s1);
        memcpy(str2, (char*)val2 + VARCHAR_LENGTH_SIZE, s2);

        return strcmp(str1, str2);
    }
    else if (attr.type == TypeInt)
    {
        memcpy(&i1, val1, INT_SIZE);
        memcpy(&i2, val2, INT_SIZE);
        return compare(i1, i2);
    }
    else if (attr.type == TypeReal)
    {
        memcpy(&f1, val1, REAL_SIZE);
        memcpy(&f2, val2, REAL_SIZE);
        return compare(f1,f2);
    }

    // Should never be here
    return -1;
}

static bool compare(CompOp op, Attribute attr, const void *val1, const void *val2)
{
    int result = compare(attr, val1, val2);
    
    switch (op)
    {
        case EQ_OP: return result == 0;
        case LT_OP: return result  < 0;
        case GT_OP: return result  > 0;
        case LE_OP: return result <= 0;
        case GE_OP: return result >= 0;
        case NE_OP: return result != 0;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}
// -----------------------------------------------------------------------------

// Functions for dealing with null indicators ----------------------------------
static void setNullBit(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] |= indicatorMask;
}

// Calculate actual bytes for nulls-indicator for the given field counts
static int getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}
// -----------------------------------------------------------------------------

Filter::Filter(Iterator* input, const Condition &condition)
: input(input), cond(condition)
{
    input->getAttributes(this->attrs);
}

RC Filter::getNextTuple(void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (input->getNextTuple(data) == QE_EOF)
        return QE_EOF;

    RC rc;
    do
    {
        // Parse out the attributes
        map<string, AttributeData> fields;
        vector<AttributeData> attrData;
        rbfm->getAttributeData(attrs, data, attrData);

        for (AttributeData ad : attrData)
        {
            fields[ad.attr.name] = ad;
        }

        // Check if condition is valid
        if (cond.bRhsIsAttr || !fields.count(cond.lhsAttr) || (cond.rhsValue.type != fields[cond.lhsAttr].attr.type))
        {
            cerr << "bad condition" << endl;
            return QE_BAD_CONDITION;
        }

        // Perform comparison
        if (compare(cond.op, fields[cond.lhsAttr].attr, fields[cond.lhsAttr].data, cond.rhsValue.data))
        {
            return SUCCESS;
        }
        // Repeat until either compare succeeds or we hit an error
    }while ((rc = input->getNextTuple(data)) == SUCCESS);
    return rc;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->attrs;
}

Project::Project(Iterator *input, const vector<string> &attrNames)
: input(input)
{
    input->getAttributes(inputAttrs);
    // Create output attributes
    for (string n : attrNames)
    {
        for (Attribute a : inputAttrs)
        {
            if (a.name == n)
            {
                outputAttrs.push_back(a);
                break;
            }
        }
    }
    tmpData = malloc(PAGE_SIZE);
}
Project::~Project()
{
    free(tmpData);
}

RC Project::getNextTuple(void *data)
{
    // Grab next tuple
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (input->getNextTuple(tmpData) == QE_EOF)
        return QE_EOF;

    // Parse the fields
    map<string, AttributeData> fields;
    vector<AttributeData> attrData;
    rbfm->getAttributeData(inputAttrs, tmpData, attrData);

    for (AttributeData ad : attrData)
    {
        fields[ad.attr.name] = ad;
    }

    // Combine the desired fields together
    join(fields, data);
    return SUCCESS;
}

void Project::join(map<string, AttributeData> fields, void *data)
{
    int nullIndicatorSize = getNullIndicatorSize(outputAttrs.size());
    unsigned offset = nullIndicatorSize;

    // Loop through outputattrs, 
    for (unsigned i = 0; i < outputAttrs.size(); i++)
    {
        // Grab each field
        string fieldName = outputAttrs[i].name;
        AttributeData attrData = fields[fieldName];

        // If null, set null bit and move on
        if (attrData.null)
        {
            setNullBit((char*)data, i);
            continue;
        }

        // else copy the field into the output
        if (attrData.attr.type == TypeInt)
        {
            memcpy((char*)data + offset, attrData.data, INT_SIZE);
            offset += INT_SIZE;
        }
        else if (attrData.attr.type == TypeReal)
        {
            memcpy((char*)data + offset, attrData.data, REAL_SIZE);
            offset += REAL_SIZE;
        }
        else
        {
            int32_t len;
            memcpy(&len, attrData.data, VARCHAR_LENGTH_SIZE);
            memcpy((char*)data + offset, attrData.data, VARCHAR_LENGTH_SIZE + len);
            offset += VARCHAR_LENGTH_SIZE + len;
        }
    }
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->outputAttrs;
}

INLJoin::INLJoin(Iterator *leftin, IndexScan *rightIn, const Condition &condition)
: leftin(leftin), rightin(rightIn), cond(condition)
{
    leftin->getAttributes(leftAttrs);
    rightin->getAttributes(rightAttrs);

    outputAttrs = leftAttrs;
    outputAttrs.insert(outputAttrs.end(), rightAttrs.begin(), rightAttrs.end());

    leftData = malloc(PAGE_SIZE);
    rightData = malloc(PAGE_SIZE);
    resultData = malloc(PAGE_SIZE);

    readLeft = true;
}

INLJoin::~INLJoin()
{
    free(leftData);
    free(rightData);
    free(resultData);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = outputAttrs;
}

RC INLJoin::getNextTuple(void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    if (readLeft)
    {
        // read in from left
        rc = leftin->getNextTuple(leftData);
        if (rc)
            return rc;
        readLeft = false;

        // Setup the first half of the result
        // this only needs to be calculated once each time we grab a left tuple
        setupResultCache();
    }

    // Grab the right tuple, if eof we try to read from the left again
    rc = rightin->getNextTuple(rightData);
    if (rc == QE_EOF)
    {
        readLeft = true;
        rightin->setIterator();
        return getNextTuple(data);
    }
    else if (rc != SUCCESS)
    {
        return rc;
    }

    // Parse the fields of the right tuple
    map<string, AttributeData> rightFields;
    vector<AttributeData> attrData;
    rbfm->getAttributeData(rightAttrs, rightData, attrData);

    for (AttributeData ad : attrData)
    {
        rightFields[ad.attr.name] = ad;
    }

    // Perform the comparison
    if (!compare(cond.op, currentAttribute.attr, currentAttribute.data, rightFields[cond.rhsAttr].data))
    {
        
        return getNextTuple(data);
    }

    // If comparison valid, we join the right tuple with our cached left tuple
    join(data, rightFields);

    return SUCCESS;
}

void INLJoin::setupResultCache()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Parse the left fields
    map<string, AttributeData> fields;
    vector<AttributeData> attrData;
    rbfm->getAttributeData(leftAttrs, leftData, attrData);

    for (AttributeData ad : attrData)
    {
        fields[ad.attr.name] = ad;
    }

    currentAttribute = fields[cond.lhsAttr];

    // read all fields into resultData
    memset(resultData, 0, PAGE_SIZE);
    this->nullIndicatorSize = getNullIndicatorSize(outputAttrs.size());
    this->offset = nullIndicatorSize;

    for (unsigned i = 0; i < leftAttrs.size(); i++)
    {
        string fieldName = leftAttrs[i].name;
        AttributeData attrData = fields[fieldName];

        if (attrData.null)
        {
            setNullBit((char*)resultData, i);
            continue;
        }

        if (attrData.attr.type == TypeInt)
        {
            memcpy((char*)resultData + offset, attrData.data, INT_SIZE);
            offset += INT_SIZE;
        }
        else if (attrData.attr.type == TypeReal)
        {
            memcpy((char*)resultData + offset, attrData.data, REAL_SIZE);
            offset += REAL_SIZE;
        }
        else
        {
            int32_t len;
            memcpy(&len, attrData.data, VARCHAR_LENGTH_SIZE);
            memcpy((char*)resultData + offset, attrData.data, VARCHAR_LENGTH_SIZE + len);
            offset += VARCHAR_LENGTH_SIZE + len;
        }
    }
}

void INLJoin::join(void *data, const map<string, AttributeData> &fields)
{
    // Copy cached results from resultData
    memcpy(data, resultData, offset);

    // Add right tuple fields to result
    unsigned tmpOffset = this->offset;
    for (unsigned i = 0; i < rightAttrs.size(); i++)
    {
        string fieldName = rightAttrs[i].name;
        AttributeData attrData = fields.find(fieldName)->second;

        if (attrData.null)
        {
            setNullBit((char*)data, i);
            continue;
        }

        if (attrData.attr.type == TypeInt)
        {
            memcpy((char*)data + tmpOffset, attrData.data, INT_SIZE);
            tmpOffset += INT_SIZE;
        }
        else if (attrData.attr.type == TypeReal)
        {
            memcpy((char*)data + tmpOffset, attrData.data, REAL_SIZE);
            tmpOffset += REAL_SIZE;
        }
        else
        {
            int32_t len;
            memcpy(&len, attrData.data, VARCHAR_LENGTH_SIZE);
            memcpy((char*)data + tmpOffset, attrData.data, VARCHAR_LENGTH_SIZE + len);
            tmpOffset += VARCHAR_LENGTH_SIZE + len;
        }
    }
}