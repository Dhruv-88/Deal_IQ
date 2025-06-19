# Deal IQ - Intelligence for used car deals 🚗💡

A Chrome extension that predicts car deal quality on marketplace websites using machine learning. This project demonstrates end-to-end ML engineering capabilities including planning, building, and deploying ML projects.


## 🎯 Project Overview

Deal IQ analyzes used car listings on popular marketplace websites and provides instant deal quality predictions, helping users identify great deals and avoid overpriced vehicles.

## ✨ Features

### 🎯 Deal Classification System

The extension categorizes deals into four distinct categories:

| Category | Criteria | Description |
|----------|----------|-------------|
| 🟢 **Great Deal** | 20%+ below fair price | Exceptional value, act fast! |
| 🔵 **Good Deal** | 10-20% below fair price | Solid deal worth considering |
| 🟡 **Fair Deal** | ±10% of fair price | Market-rate pricing |
| 🔴 **Overpriced** | 10%+ above fair price | Above market value |

### 📊 Output Information

For each analyzed listing, Deal IQ provides:
- **Deal Classification Label** with color coding
- **Confidence Score** (0-100%)
- **Percentage Difference** from estimated market value
- **Estimated Fair Price** based on ML model prediction

## 🌐 Supported Platforms

### Primary Targets
- **Facebook Marketplace** - Meta's vehicle marketplace
- **Clutch** - Canadian automotive marketplace

### Scope
- **Vehicle Type**: Used cars only
- **Geographic Coverage**: USA-based listings
- **Training Data**: 2017-2022 vehicle listings

## 🔧 Technical Features

### Machine Learning Model Features

#### Core Features (Primary Predictors)
- **Price** - Listed vehicle price
- **Year** - Manufacturing year
- **Manufacturer** - Vehicle brand
- **Model** - Specific vehicle model
- **Odometer** - Mileage reading
- **Condition** - Vehicle condition rating

#### Secondary Features
- **Cylinders** - Engine cylinder count
- **Fuel Type** - Gasoline, diesel, hybrid, electric
- **Title Status** - Clean, salvage, rebuilt, etc.
- **Transmission** - Manual, automatic, CVT
- **Drive Type** - FWD, RWD, AWD, 4WD
- **Vehicle Size** - Compact, mid-size, full-size
- **Vehicle Type** - Sedan, SUV, truck, coupe

#### Location Features
- **Region** - Geographic region
- **State** - US state
- **Coordinates** - Latitude and longitude
- **Paint Color** - Vehicle exterior color
- **Description** - Text analysis of listing description

## 🎮 User Experience

### Current Implementation
- **Activation Method**: Manual trigger via button click
- **Display Format**: Overlay or popup on listing page
- **Information Display**: 
  - Deal category badge
  - Fair price estimate
  - Percentage variance from market value
  - Confidence indicator

### Future Enhancements
- **Automatic Activation**: Auto-trigger after API stability testing
- **Batch Analysis**: Analyze multiple listings simultaneously
- **Price Alerts**: Notify when great deals appear
- **Historical Tracking**: Track price changes over time

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Conda or Miniconda
- Poetry
- Chrome browser for extension testing

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Dhruv-88/Deal_IQ.git
   cd Deal_IQ
   